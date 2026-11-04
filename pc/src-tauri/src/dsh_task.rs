/*
 * [Input] HachimoDock DSH bridge plugin task API (POST /task, GET /task, POST /interrupt).
 * [Output] Loopback-only task dispatch, status and interrupt calls with fixed error mapping
 *          and a one-shot GET takeover probe for ambiguous POST /task outcomes.
 * [Pos] Host-side task transport for realtime DSH delegation; session UI stays on Agent Bus.
 * [Sync] If integrations/dsh-hachimodock task routes change, update this file and its tests.
 */

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

const DEFAULT_BRIDGE_URL: &str = "http://127.0.0.1:17892";
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(3);
const STATUS_TIMEOUT: Duration = Duration::from_secs(2);
const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(2);
const FOCUS_TIMEOUT: Duration = Duration::from_millis(800);
/// Bounded wait for the single takeover probe after an uncertain POST.
const TAKEOVER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Resolve the DSH bridge origin, stripping a trailing slash.
pub(crate) fn bridge_url() -> String {
    let raw = std::env::var("HACHIMODOCK_DSH_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_BRIDGE_URL.to_string());
    raw.trim_end_matches('/').to_string()
}

/// One task dispatch request sent to the DSH bridge plugin.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DispatchRequest {
    pub task_id: String,
    pub session_id: String,
    pub text: String,
    pub origin: String,
    pub metadata: Value,
}

/// Public task snapshot returned by `POST /task` and `GET /task`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TaskSnapshot {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub queued: bool,
    #[serde(default)]
    pub turn: Option<i64>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub result_excerpt: String,
    #[serde(default)]
    pub accepted_at: u64,
    #[serde(default)]
    pub updated_at: u64,
}

/// Last DSH Web/Desktop main-session selection. Contains no title/content.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FocusSnapshot {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub source: String,
    /// "user" or "restore"; a restored selection must not override a newer
    /// explicit P4 choice after a DSH renderer restart.
    #[serde(default)]
    pub origin: String,
}

/// One sampled DSH UI focus state. `Ok` is authoritative for the current
/// bridge instance; `Unsupported` is an older plugin without `/focus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FocusSampleStatus {
    Ok,
    Unsupported,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FocusSample {
    pub status: FocusSampleStatus,
    pub focus: Option<FocusSnapshot>,
    /// Diagnostic only: an SSE client can be attached without a focus observer.
    pub client_attached: bool,
    /// Separate focus-observer capability marker from POST /focus heartbeats.
    pub observer_attached: bool,
    pub observer_capable: bool,
    pub instance_id: String,
    pub bridge_reachable: bool,
}

/// Dispatch result. `adopted` means the POST response was lost but the
/// one-shot GET probe found the task; the caller must keep monitoring it and
/// must not send the prompt again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DispatchOutcome {
    pub snapshot: TaskSnapshot,
    pub adopted: bool,
}

/// Fixed task transport failures. The desktop agent maps these to user-facing
/// Chinese receipts; raw plugin/HTTP text never reaches the conversation model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchError {
    /// Could not connect to the bridge at all; the request was never sent.
    Unavailable,
    /// The plugin answered 503: the task route/adapter is not available.
    BridgeUnavailable,
    /// The plugin rejected the request body with 413.
    PayloadTooLarge,
    SessionNotFound,
    AgentNotLive,
    DuplicateConflict,
    Invalid,
    FollowupFailed,
    /// POST /task may have been accepted, but the single GET probe could not
    /// find or read the task. The caller reports unknown and never re-posts.
    Uncertain,
    /// Malformed task status response while polling.
    Unknown,
}

impl DispatchError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Unavailable | Self::BridgeUnavailable => "DSH_BRIDGE_UNAVAILABLE",
            Self::PayloadTooLarge => "DSH_TEXT_TOO_LARGE",
            Self::SessionNotFound => "DSH_NO_LIVE_SESSION",
            Self::AgentNotLive => "DSH_TARGET_NOT_LIVE",
            Self::DuplicateConflict => "DSH_DUPLICATE",
            Self::Invalid => "DSH_INVALID_TASK",
            Self::FollowupFailed => "DSH_DISPATCH_FAILED",
            Self::Uncertain | Self::Unknown => "DSH_STATUS_UNKNOWN",
        }
    }
}

fn client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_millis(800))
        .timeout(timeout)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn error_code(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default()
}

fn map_dispatch_status(status: u16, body: &str) -> DispatchError {
    match status {
        400 => DispatchError::Invalid,
        409 => match error_code(body).as_str() {
            "session-not-found" => DispatchError::SessionNotFound,
            "agent-not-live" => DispatchError::AgentNotLive,
            "duplicate-conflict" => DispatchError::DuplicateConflict,
            _ => DispatchError::FollowupFailed,
        },
        413 => DispatchError::PayloadTooLarge,
        500 => DispatchError::FollowupFailed,
        503 => DispatchError::BridgeUnavailable,
        _ => DispatchError::Uncertain,
    }
}

fn parse_dispatch_body(body: &str) -> Result<TaskSnapshot, DispatchError> {
    let value: Value = serde_json::from_str(body).map_err(|_| DispatchError::Uncertain)?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(DispatchError::Uncertain);
    }
    serde_json::from_value(value).map_err(|_| DispatchError::Uncertain)
}

/// One bounded GET probe after an ambiguous POST. Exactly one request is made;
/// a miss or probe failure returns `Uncertain` and never triggers a re-POST.
async fn takeover_once(origin: &str, task_id: &str) -> Result<DispatchOutcome, DispatchError> {
    match status_at_with_timeout(origin, task_id, TAKEOVER_PROBE_TIMEOUT).await {
        Ok(Some(snapshot)) => Ok(DispatchOutcome { snapshot, adopted: true }),
        Ok(None) | Err(_) => Err(DispatchError::Uncertain),
    }
}

async fn dispatch_at_with_timeout(
    origin: &str,
    request: &DispatchRequest,
    timeout: Duration,
) -> Result<DispatchOutcome, DispatchError> {
    let response = match client(timeout)
        .post(format!("{origin}/task"))
        .json(request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            // A pure connect failure never reached the plugin. Everything else
            // (timeout, reset, body loss) may have been accepted: take over.
            if error.is_connect() {
                return Err(DispatchError::Unavailable);
            }
            return takeover_once(origin, &request.task_id).await;
        }
    };
    let status = response.status().as_u16();
    let body = match response.text().await {
        Ok(body) => body,
        Err(_) => return takeover_once(origin, &request.task_id).await,
    };
    if status == 200 {
        return match parse_dispatch_body(&body) {
            Ok(snapshot) => Ok(DispatchOutcome { snapshot, adopted: false }),
            Err(_) => takeover_once(origin, &request.task_id).await,
        };
    }
    if matches!(status, 502 | 504) || status >= 505 {
        return takeover_once(origin, &request.task_id).await;
    }
    Err(map_dispatch_status(status, &body))
}

/// Submit one tracked task to the DSH bridge plugin.
///
/// An ambiguous POST outcome is followed by exactly one GET /task probe with
/// the same client taskId. If the plugin accepted the task before losing the
/// response, the returned outcome is `adopted: true`; the caller continues
/// monitoring and never sends the prompt again.
pub(crate) async fn dispatch(request: &DispatchRequest) -> Result<DispatchOutcome, DispatchError> {
    dispatch_at_with_timeout(&bridge_url(), request, DISPATCH_TIMEOUT).await
}

async fn status_at_with_timeout(
    origin: &str,
    task_id: &str,
    timeout: Duration,
) -> Result<Option<TaskSnapshot>, DispatchError> {
    let response = client(timeout)
        .get(format!("{origin}/task"))
        .query(&[("taskId", task_id)])
        .send()
        .await
        .map_err(|_| DispatchError::Unavailable)?;
    let status = response.status().as_u16();
    let body = response.text().await.map_err(|_| DispatchError::Unavailable)?;
    if status == 404 {
        return Ok(None);
    }
    if status != 200 {
        return Err(DispatchError::Unavailable);
    }
    let value: Value = serde_json::from_str(&body).map_err(|_| DispatchError::Unknown)?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(DispatchError::Unknown);
    }
    let task = value.get("task").cloned().ok_or(DispatchError::Unknown)?;
    Ok(Some(serde_json::from_value(task).map_err(|_| DispatchError::Unknown)?))
}

/// Read one tracked task. `None` means the plugin no longer knows the id
/// (restart, expiry) and the caller must not re-dispatch.
pub(crate) async fn status(task_id: &str) -> Result<Option<TaskSnapshot>, DispatchError> {
    status_at_with_timeout(&bridge_url(), task_id, STATUS_TIMEOUT).await
}

async fn focus_at(origin: &str) -> FocusSample {
    let unavailable = FocusSample {
        status: FocusSampleStatus::Unavailable,
        focus: None,
        client_attached: false,
        observer_attached: false,
        observer_capable: false,
        instance_id: String::new(),
        bridge_reachable: false,
    };
    let response = match client(FOCUS_TIMEOUT)
        .get(format!("{origin}/focus"))
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return unavailable,
    };
    let status = response.status().as_u16();
    if status == 404 {
        return FocusSample {
            status: FocusSampleStatus::Unsupported,
            focus: None,
            client_attached: false,
            observer_attached: false,
            observer_capable: false,
            instance_id: String::new(),
            bridge_reachable: true,
        };
    }
    let body = match response.text().await {
        Ok(body) => body,
        Err(_) => {
            return FocusSample {
                bridge_reachable: true,
                ..unavailable
            }
        }
    };
    if status != 200 {
        return FocusSample {
            bridge_reachable: true,
            ..unavailable
        };
    }
    let value: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(_) => {
            return FocusSample {
                bridge_reachable: true,
                ..unavailable
            }
        }
    };
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return FocusSample {
            bridge_reachable: true,
            ..unavailable
        };
    }
    let focus = match value.get("focus") {
        None => None,
        Some(Value::Null) => None,
        Some(Value::Object(_)) => match serde_json::from_value::<FocusSnapshot>(
            value.get("focus").cloned().unwrap_or(Value::Null),
        ) {
            Ok(focus) => Some(focus),
            Err(_) => {
                return FocusSample {
                    bridge_reachable: true,
                    ..unavailable
                }
            }
        },
        Some(_) => {
            return FocusSample {
                bridge_reachable: true,
                ..unavailable
            }
        }
    };
    FocusSample {
        status: FocusSampleStatus::Ok,
        focus,
        client_attached: value.get("clientAttached").and_then(Value::as_bool) == Some(true),
        observer_attached: value.get("observerAttached").and_then(Value::as_bool) == Some(true),
        observer_capable: value.get("observerCapable").and_then(Value::as_bool) == Some(true),
        instance_id: value
            .get("instanceId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        bridge_reachable: true,
    }
}

fn interrupt_at(origin: &str, session_id: &str) -> Result<bool, String> {
    let response = reqwest::blocking::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_millis(800))
        .timeout(INTERRUPT_TIMEOUT)
        .build()
        .map_err(|error| format!("DSH interrupt client failed: {error}"))?
        .post(format!("{origin}/interrupt"))
        .json(&json!({ "sessionId": session_id }))
        .send()
        .map_err(|error| format!("DSH interrupt request failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("DSH interrupt HTTP {}", response.status().as_u16()));
    }
    let body = response.text().unwrap_or_default();
    let value: Value =
        serde_json::from_str(&body).map_err(|_| "DSH interrupt response invalid".to_string())?;
    Ok(value.get("interrupted").and_then(Value::as_bool).unwrap_or(true))
}

/// Sample the DSH Web/Desktop main-session selection at realtime start.
pub(crate) async fn focus() -> FocusSample {
    focus_at(&bridge_url()).await
}

/// Cancel the active DSH turn through the plugin (blocking helper for the USB
/// and realtime threads). `Ok(false)` means the plugin reported no active turn.
pub(crate) fn interrupt(session_id: &str) -> Result<bool, String> {
    interrupt_at(&bridge_url(), session_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn one_shot_server(status: u16, body: &'static str, expect_path: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 8192];
            let read = socket.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            assert!(request.contains(expect_path), "unexpected request: {request}");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        origin
    }

    /// Server that receives a POST, withholds/drops the response, then serves
    /// exactly one GET probe. Returns the origin and the observed request list.
    fn takeover_server(
        hold_ms: u64,
        get_status: u16,
        get_body: &'static str,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 8192];
            let read = first.read(&mut buffer).unwrap();
            observed
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buffer[..read]).to_string());
            if hold_ms > 0 {
                thread::sleep(Duration::from_millis(hold_ms));
            }
            drop(first); // response lost / connection reset
            let (mut second, _) = listener.accept().unwrap();
            let read = second.read(&mut buffer).unwrap();
            observed
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buffer[..read]).to_string());
            second
                .write_all(
                    format!(
                        "HTTP/1.1 {get_status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{get_body}",
                        get_body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        (origin, requests)
    }

    fn request() -> DispatchRequest {
        DispatchRequest {
            task_id: "task-1".into(),
            session_id: "auto".into(),
            text: "修复测试".into(),
            origin: "realtime-voice".into(),
            metadata: json!({"board":"p4"}),
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn accepted_task_json() -> &'static str {
        r#"{"ok":true,"taskId":"task-1","sessionId":"session-1","state":"accepted","queued":false,"turn":null,"reason":"","resultExcerpt":"","acceptedAt":1,"updatedAt":2}"#
    }

    fn probe_task_json() -> &'static str {
        r#"{"ok":true,"task":{"taskId":"task-1","sessionId":"session-1","state":"accepted","queued":false,"turn":null,"reason":"","resultExcerpt":"","acceptedAt":1,"updatedAt":2}}"#
    }

    #[test]
    fn dispatch_parses_accepted_task_and_sends_the_contract_body() {
        let origin = one_shot_server(200, accepted_task_json(), "POST /task");
        let outcome = runtime()
            .block_on(dispatch_at_with_timeout(&origin, &request(), Duration::from_secs(1)))
            .unwrap();
        assert!(!outcome.adopted);
        assert_eq!(outcome.snapshot.task_id, "task-1");
        assert_eq!(outcome.snapshot.session_id, "session-1");
        assert_eq!(outcome.snapshot.state, "accepted");
    }

    #[test]
    fn dispatch_maps_definite_plugin_errors_without_probing() {
        let cases = [
            (409, "session-not-found", DispatchError::SessionNotFound),
            (409, "agent-not-live", DispatchError::AgentNotLive),
            (409, "duplicate-conflict", DispatchError::DuplicateConflict),
            (400, "invalid-request", DispatchError::Invalid),
            (413, "payload-too-large", DispatchError::PayloadTooLarge),
            (500, "followup-failed", DispatchError::FollowupFailed),
            (503, "bridge-unavailable", DispatchError::BridgeUnavailable),
        ];
        for (status, code, expected) in cases {
            let body = format!(r#"{{"ok":false,"error":"{code}","message":"internal detail"}}"#);
            let leaked = Box::leak(body.into_boxed_str());
            let origin = one_shot_server(status, leaked, "POST /task");
            assert_eq!(
                runtime().block_on(dispatch_at_with_timeout(
                    &origin,
                    &request(),
                    Duration::from_secs(1)
                )),
                Err(expected),
                "{status} {code}"
            );
        }
    }

    #[test]
    fn uncertain_connection_loss_takes_over_exactly_once_without_a_second_post() {
        let (origin, requests) = takeover_server(0, 200, probe_task_json());
        let outcome = runtime()
            .block_on(dispatch_at_with_timeout(&origin, &request(), Duration::from_secs(1)))
            .unwrap();
        assert!(outcome.adopted, "the GET probe must adopt the accepted task");
        assert_eq!(outcome.snapshot.task_id, "task-1");
        let observed = requests.lock().unwrap();
        assert_eq!(observed.len(), 2, "exactly one POST + one GET probe: {observed:?}");
        assert!(observed[0].contains("POST /task"));
        assert!(observed[1].contains("GET /task?taskId=task-1"), "{}", observed[1]);
        assert_eq!(
            observed.iter().filter(|line| line.starts_with("POST ")).count(),
            1,
            "must never re-POST the same taskId"
        );
    }

    #[test]
    fn uncertain_timeout_takes_over_without_a_duplicate_followup() {
        let (origin, requests) = takeover_server(250, 200, probe_task_json());
        let outcome = runtime()
            .block_on(dispatch_at_with_timeout(
                &origin,
                &request(),
                Duration::from_millis(80),
            ))
            .unwrap();
        assert!(outcome.adopted);
        let observed = requests.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed[0].contains("POST /task"));
        assert!(observed[1].contains("GET /task?taskId=task-1"));
        assert_eq!(observed.iter().filter(|line| line.starts_with("POST ")).count(), 1);
    }

    #[test]
    fn uncertain_probe_miss_becomes_uncertain_never_dispatched() {
        let (origin, requests) = takeover_server(0, 404, r#"{"ok":false,"error":"task-not-found"}"#);
        let error = runtime()
            .block_on(dispatch_at_with_timeout(&origin, &request(), Duration::from_secs(1)))
            .unwrap_err();
        assert_eq!(error, DispatchError::Uncertain);
        let observed = requests.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed[1].contains("GET /task?taskId=task-1"));
        assert_eq!(observed.iter().filter(|line| line.starts_with("POST ")).count(), 1);
    }

    #[test]
    fn focus_parses_the_selection_without_content() {
        let origin = one_shot_server(
            200,
            r#"{"ok":true,"focus":{"sessionId":"session-a","updatedAt":1790000000000,"seq":4,"source":"dsh-ui","origin":"user"},"clientAttached":true,"observerAttached":true,"observerCapable":true,"instanceId":"bridge-1"}"#,
            "GET /focus",
        );
        let sample = runtime().block_on(focus_at(&origin));
        assert_eq!(sample.status, FocusSampleStatus::Ok);
        assert_eq!(sample.focus.as_ref().unwrap().session_id, "session-a");
        assert_eq!(sample.focus.as_ref().unwrap().seq, 4);
        assert_eq!(sample.focus.as_ref().unwrap().origin, "user");
        assert_eq!(sample.client_attached, true);
        assert_eq!(sample.observer_attached, true);
        assert_eq!(sample.observer_capable, true);
        assert_eq!(sample.instance_id, "bridge-1");
    }

    #[test]
    fn focus_parses_an_explicit_clear_and_legacy_404() {
        let origin = one_shot_server(
            200,
            r#"{"ok":true,"focus":null,"clientAttached":true,"observerAttached":true,"observerCapable":true,"instanceId":"bridge-1"}"#,
            "GET /focus",
        );
        let cleared = runtime().block_on(focus_at(&origin));
        assert_eq!(cleared.status, FocusSampleStatus::Ok);
        assert_eq!(cleared.focus, None);
        assert_eq!(cleared.client_attached, true);
        assert_eq!(cleared.observer_attached, true);

        let legacy = one_shot_server(404, r#"{"ok":false,"error":"not-found"}"#, "GET /focus");
        let unsupported = runtime().block_on(focus_at(&legacy));
        assert_eq!(unsupported.status, FocusSampleStatus::Unsupported);
        assert_eq!(unsupported.focus, None);
    }

    #[test]
    fn focus_distinguishes_bridge_error_from_unreachable() {
        let error = one_shot_server(500, r#"{"ok":false,"error":"internal"}"#, "GET /focus");
        let reachable = runtime().block_on(focus_at(&error));
        assert_eq!(reachable.status, FocusSampleStatus::Unavailable);
        assert_eq!(reachable.bridge_reachable, true);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let refused = runtime().block_on(focus_at(&origin));
        assert_eq!(refused.status, FocusSampleStatus::Unavailable);
        assert_eq!(refused.bridge_reachable, false);
    }

    #[test]
    fn status_treats_404_as_unknown_without_retrying() {
        let origin = one_shot_server(404, r#"{"ok":false,"error":"task-not-found"}"#, "GET /task");
        assert_eq!(
            runtime().block_on(status_at_with_timeout(
                &origin,
                "task-1",
                Duration::from_secs(1)
            )),
            Ok(None)
        );
    }

    #[test]
    fn status_reads_nested_task_body() {
        let origin = one_shot_server(
            200,
            r#"{"ok":true,"task":{"taskId":"task-1","sessionId":"session-1","state":"done","queued":false,"turn":4,"reason":"completed","resultExcerpt":"已完成","acceptedAt":1,"updatedAt":9}}"#,
            "GET /task",
        );
        let snapshot = runtime()
            .block_on(status_at_with_timeout(&origin, "task-1", Duration::from_secs(1)))
            .unwrap()
            .expect("task");
        assert_eq!(snapshot.state, "done");
        assert_eq!(snapshot.turn, Some(4));
        assert_eq!(snapshot.result_excerpt, "已完成");
    }

    #[test]
    fn interrupt_reports_no_active_turn() {
        let origin = one_shot_server(
            200,
            r#"{"ok":true,"sessionId":"session-1","interrupted":false,"state":"idle"}"#,
            "POST /interrupt",
        );
        assert_eq!(interrupt_at(&origin, "session-1"), Ok(false));
    }

    #[test]
    fn interrupt_reports_the_active_turn() {
        let origin = one_shot_server(
            200,
            r#"{"ok":true,"sessionId":"session-1","interrupted":true,"state":"cancelled"}"#,
            "POST /interrupt",
        );
        assert_eq!(interrupt_at(&origin, "session-1"), Ok(true));
    }

    #[test]
    fn bridge_url_falls_back_to_the_contract_port() {
        let url = bridge_url();
        assert!(url.starts_with("http://"));
        assert!(!url.ends_with('/'));
    }
}
