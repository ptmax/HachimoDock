/*
 * [Input] DSH bridge plugin approval SSE events and ESP32-P4 USB approval decisions.
 * [Output] Single-request approval relay: plugin -> USB approval/request -> device
 *          decision -> plugin /approval/decision, with bounded timeout and no
 *          fallback that blocks the normal DSH browser approval path.
 * [Pos] Host-side R5 approval relay for the DeepSeek Harness channel.
 * [Sync] If this contract changes, update docs/dsh-integration-contract.md and
 *         firmware/protocol.md.
 */

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

use crate::usb_serial::UsbSerialManager;

const DEFAULT_BRIDGE_URL: &str = "http://127.0.0.1:17892";
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(30);

type DecisionSender = mpsc::Sender<String>;

fn pending_decisions() -> &'static Mutex<HashMap<String, DecisionSender>> {
    static PENDING: OnceLock<Mutex<HashMap<String, DecisionSender>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn bridge_base_url() -> String {
    std::env::var("HACHIMODOCK_DSH_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_BRIDGE_URL.to_string())
}

/// Convert one plugin `approval` SSE payload into the bounded USB payload. The
/// device only needs presentation data; requestId is the correlation key.
fn device_request_payload(event: &Value) -> Option<Value> {
    let request_id = event
        .get("requestId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let agent = event
        .get("agent")
        .or_else(|| event.get("agentId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("DeepSeek Harness");
    let request = json!({
        "requestId": request_id,
        "agent": agent,
        "tool": event.get("tool").and_then(Value::as_str).unwrap_or(""),
        "reason": event
            .get("displayReason")
            .or_else(|| event.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or(""),
        "sessionId": event.get("sessionId").and_then(Value::as_str).unwrap_or(""),
        "expiresAtMs": event.get("expiresAtMs").and_then(Value::as_u64).unwrap_or(0),
    });
    Some(request)
}

fn parse_sse_block(event: &str, data: &str) -> Option<Value> {
    if event.trim() != "approval" {
        return None;
    }
    serde_json::from_str::<Value>(data.trim()).ok()
}

/// Parse one SSE line into `(event, data)` state. Returns true when the blank
/// separator line completes an event and the caller should dispatch it.
fn consume_sse_line(line: &str, event: &mut String, data: &mut String) -> bool {
    if line.is_empty() {
        return true;
    }
    if let Some(value) = line.strip_prefix("event:") {
        *event = value.trim().to_string();
    } else if let Some(value) = line.strip_prefix("data:") {
        if !data.is_empty() {
            data.push('\n');
        }
        data.push_str(value.trim_start());
    }
    false
}

fn post_decision(base_url: &str, request_id: &str, decision: &str) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| format!("DSH approval client failed: {error}"))?;
    let response = client
        .post(format!("{base_url}/approval/decision"))
        .json(&json!({ "requestId": request_id, "decision": decision }))
        .send()
        .map_err(|error| format!("DSH approval decision failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "DSH approval decision HTTP {}",
            response.status().as_u16()
        ));
    }
    Ok(())
}

fn wait_for_device_decision(
    usb: UsbSerialManager,
    request: Value,
    base_url: String,
) {
    let request_id = request
        .get("requestId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if request_id.is_empty() {
        return;
    }
    let (sender, receiver) = mpsc::channel::<String>();
    if let Ok(mut pending) = pending_decisions().lock() {
        pending.insert(request_id.clone(), sender);
    }
    if let Err(error) = usb.send("approval/request", &request) {
        eprintln!("[dsh-approval] usb approval/request failed: {error}");
        if let Ok(mut pending) = pending_decisions().lock() {
            pending.remove(&request_id);
        }
        return;
    }
    match receiver.recv_timeout(APPROVAL_TIMEOUT) {
        Ok(decision) => {
            if let Err(error) = post_decision(&base_url, &request_id, &decision) {
                eprintln!("[dsh-approval] {error}");
            }
        }
        Err(_) => {
            eprintln!("[dsh-approval] device decision timeout for {request_id}");
        }
    }
    if let Ok(mut pending) = pending_decisions().lock() {
        pending.remove(&request_id);
    }
}

/// Handle a device-originated approval decision. Returns true when the topic is
/// claimed by this relay (including unknown/stale requestIds, which are ignored
/// after the bridge has already timed out).
pub fn handle_device_decision(payload: &Value) -> bool {
    let request_id = payload
        .get("requestId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    let decision = payload
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if request_id.is_empty() || (decision != "allow" && decision != "deny") {
        eprintln!("[dsh-approval] ignored malformed device decision");
        return true;
    }
    let sender = pending_decisions()
        .lock()
        .ok()
        .and_then(|mut pending| pending.remove(&request_id));
    if let Some(sender) = sender {
        let normalized = if decision == "allow" { "allow" } else { "deny" };
        let _ = sender.send(normalized.to_string());
    } else {
        eprintln!("[dsh-approval] ignored stale decision {request_id}");
    }
    true
}

fn run_event_stream(usb: UsbSerialManager, base_url: String) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10 * 60))
        .build()
        .map_err(|error| format!("DSH SSE client failed: {error}"))?;
    let response = client
        .get(format!("{base_url}/events"))
        .send()
        .map_err(|error| format!("DSH SSE connect failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("DSH SSE HTTP {}", response.status().as_u16()));
    }
    eprintln!("[dsh-approval] listening on {base_url}/events");
    let mut event = String::new();
    let mut data = String::new();
    let reader = BufReader::new(response);
    for line in reader.lines() {
        let line = line.map_err(|error| format!("DSH SSE read failed: {error}"))?;
        if consume_sse_line(&line, &mut event, &mut data) {
            if let Some(payload) = parse_sse_block(&event, &data) {
                if let Some(request) = device_request_payload(&payload) {
                    let usb = usb.clone();
                    let base_url = base_url.clone();
                    thread::spawn(move || wait_for_device_decision(usb, request, base_url));
                }
            }
            event.clear();
            data.clear();
        }
    }
    Err("DSH SSE stream ended".to_string())
}

/// Background relay for the DSH plugin. It reconnects forever; USB absence is
/// reported and the plugin keeps its normal browser approval path.
pub fn start(usb: UsbSerialManager) {
    thread::spawn(move || loop {
        let base_url = bridge_base_url();
        if let Err(error) = run_event_stream(usb.clone(), base_url) {
            eprintln!("[dsh-approval] {error}; retrying in 3s");
        }
        thread::sleep(Duration::from_secs(3));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_tracks_approval_event() {
        let mut event = String::new();
        let mut data = String::new();
        assert!(!consume_sse_line("event: approval", &mut event, &mut data));
        assert!(!consume_sse_line(
            "data: {\"requestId\":\"req-1\"}",
            &mut event,
            &mut data
        ));
        assert!(consume_sse_line("", &mut event, &mut data));
        assert_eq!(event, "approval");
        assert_eq!(data, "{\"requestId\":\"req-1\"}");
        assert!(parse_sse_block(&event, &data).is_some());
    }

    #[test]
    fn device_payload_uses_display_reason_and_defaults() {
        let event = json!({
            "requestId": "req-1",
            "agentId": "deepseek-harness",
            "tool": "bash",
            "reason": "raw",
            "displayReason": "需要执行命令",
            "expiresAtMs": 123,
        });
        let payload = device_request_payload(&event).expect("payload");
        assert_eq!(payload["requestId"], "req-1");
        assert_eq!(payload["agent"], "deepseek-harness");
        assert_eq!(payload["reason"], "需要执行命令");
        assert_eq!(payload["expiresAtMs"], 123);
    }

    #[test]
    fn malformed_or_non_approval_events_are_ignored() {
        assert!(device_request_payload(&json!({"tool": "bash"})).is_none());
        assert!(parse_sse_block("session", "{\"type\":\"session\"}").is_none());
        assert!(parse_sse_block("approval", "not-json").is_none());
    }

    #[test]
    fn device_decision_claims_unknown_request() {
        // Topic ownership is deliberate: stale/unknown decisions must not fall
        // through to generic USB message handling.
        assert!(handle_device_decision(&json!({"requestId":"missing","decision":"allow"})));
        assert!(handle_device_decision(&json!({"requestId":"","decision":"allow"})));
        assert!(handle_device_decision(&json!({"requestId":"req","decision":"maybe"})));
    }
}
