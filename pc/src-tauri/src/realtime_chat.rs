/*
 * [Input] A start request from the UI (board id, appearance id, persona system prompt, greeting,
 *         voice spec), global saved volume, targeted saved-persona updates, continuous 16 kHz PCM frames from the board's `audio/chunk` stream while a
 *         session is live, `realtime_chat` button events, and stop requests.
 * [Output] The realtime persona conversation loop: energy VAD → Doubao streaming ASR → persona LLM
 *          (streamed) → Doubao TTS sentences → paced `audio/play_*` playback on the board, plus
 *          `ui/conversation` HUD updates, `realtime-chat-state` events for the UI, and the
 *          `realtime-chat-request` event that lets the UI resolve the active appearance when the
 *          board key is pressed. AEC/VAD-capable boards stay listening during replies;
 *          candidate speech is checked by streaming ASR before barge-in cancels generation,
 *          TTS and queued playback. Empty/noise candidates leave the reply intact, except a
 *          bounded short periodic near-end command whose empty Final is the only evidence.
 *          Global gain applies once at the output boundary.
 * [Pos] Tauri-side orchestrator for 实时对话, with current-window Codex delegation,
 *       bounded session-local handoff history and isolated result announcements.
 * [Sync] Update `pc/.folder.md` and `pc/docs/realtime-chat.md`; wire changes also update `firmware/protocol.md`.
 */

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::doubao_tts::{scale_pcm16, DoubaoTtsClient};
use crate::persona_llm::{self, ChatTurn, SentenceSplitter};
use crate::usb_serial::UsbSerialManager;
use crate::voice_chat_settings;
use crate::realtime_chat_log as diagnostics;
use crate::volcengine_asr::{StreamingSpeechEvent, StreamingSpeechRecognizer};

#[path = "realtime_barge.rs"]
mod barge_policy;

pub const STATE_EVENT: &str = "realtime-chat-state";
pub const REQUEST_EVENT: &str = "realtime-chat-request";
pub const ACTION_ID: &str = "realtime_chat";

const FRAME_BYTES: usize = 640; // 20 ms @ 16 kHz mono s16le
const PREROLL_FRAMES: usize = 30; // 600 ms, includes device VAD debounce and PC confirmation
const SPEECH_START_FRAMES: usize = 3; // 60 ms above threshold
const SPEECH_END_FRAMES: usize = 35; // 700 ms of silence
const MAX_UTTERANCE_MS: u64 = 15_000;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const ASR_FINAL_TIMEOUT: Duration = Duration::from_secs(8);
const FINAL_CAPTURE_LIMIT: usize = 576; // > 8 s final wait + existing 128-frame ingress
const POST_SPEAK_GUARD: Duration = Duration::from_millis(250);
const PLAYBACK_LEAD_MS: u64 = 1200;
const PLAY_CHUNK_BYTES: usize = 3200; // 100 ms
// Bounded recovery for the diagnosed long-reply USB CDC stall: a tty write can
// time out for a few hundred ms while the board stays alive, so a late 100 ms
// chunk is dropped and the session continues. Each frame already has a bounded
// transport retry (8 x 1.5 s); this only bounds how many consecutive stalled
// chunks may be dropped before the session fails closed.
const AUDIO_WRITE_MAX_CONSECUTIVE_FAILURES: u32 = 3;
const VAD_ABS_MIN: f32 = 380.0;
const VAD_NOISE_RATIO: f32 = 2.6;
// AEC can leave tiny speech-shaped residuals that WebRTC VAD labels as speech.
// This is a signal-quality floor, not a playback mute: near-end speech is still
// evaluated on every frame while the speaker is active.
const PROCESSED_VAD_ABS_MIN: f32 = 160.0;
const PROCESSED_SPEECH_START_FRAMES: usize = 8; // 160 ms; reject brief AEC onset transients
const BARGE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(3);
const BARGE_CONFIRM_MAX_FRAMES: usize = 150; // bounded even when Windows delivers queued frames in a burst
// Short commands ("停", "等等") end well before the reply-level VAD declares End
// and before the frame cap. Once the barge has voice evidence, a short
// no-new-voiced window is enough to ask ASR for its Final transcript instead of
// polling a stream that was never finalized until the confirmation deadline.
const BARGE_FINISH_MIN_VOICED_FRAMES: usize = 8; // same voice evidence as the barge gate
const BARGE_FINISH_SILENCE_FRAMES: usize = 40; // 800 ms of 20 ms frames without new voiced audio
const LISTENING_VAD_ABS_MIN: f32 = 96.0;
const LISTENING_SPEECH_START_FRAMES: usize = 5; // 100 ms; reject isolated low-energy hints
const PREVIEW_MAX_SECONDS: u64 = 20;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct VoiceSpec {
    pub speaker: String,
    pub clone_speaker_id: String,
    pub speed: f32,
    pub volume: f32,
}

impl VoiceSpec {
    fn speed(&self) -> f32 {
        if self.speed.is_finite() && self.speed > 0.0 { self.speed } else { 1.0 }
    }
    fn volume(&self) -> f32 {
        if self.volume.is_finite() && self.volume > 0.0 { self.volume } else { 1.0 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeChatStartInput {
    pub board_device_id: String,
    pub appearance_id: String,
    pub display_name: String,
    pub system_prompt: String,
    #[serde(default)]
    pub greeting: String,
    #[serde(default)]
    pub voice: VoiceSpec,
    /// P4 session-selection snapshot captured when realtime chat starts. The
    /// UI publishes this synchronously with the selection, before the
    /// asynchronous `set_p4_session_binding` commit, so a dispatch cannot race
    /// back to the previous/current session.
    #[serde(default)]
    pub target_agent_id: String,
    #[serde(default)]
    pub target_session_id: String,
    /// When the P4 target was selected (epoch ms); 0 means auto/no P4 action.
    #[serde(default)]
    pub target_updated_at: u64,
    /// Set when a DSH-following realtime start could not confirm any target;
    /// dispatch must fail closed instead of using the current session.
    #[serde(default)]
    pub target_unconfirmed: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersonaVoicePreviewInput {
    pub text: String,
    #[serde(default)]
    pub voice: VoiceSpec,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeChatStatus {
    pub active: bool,
    pub state: String,
    pub session_id: String,
    pub board_device_id: String,
    pub appearance_id: String,
    pub display_name: String,
    pub transcript: String,
    pub reply: String,
    pub error: String,
    pub reason: String,
    pub started_at_ms: u64,
    pub turns: u32,
    pub search_state: String,
    pub search_error: String,
}

fn clear_search(status: &mut RealtimeChatStatus) {
    status.search_state.clear();
    status.search_error.clear();
}

fn update_search(status: &mut RealtimeChatStatus, event: crate::web_search::Event) {
    match event {
        crate::web_search::Event::Started => { clear_search(status); status.search_state="searching".into(); },
        crate::web_search::Event::Finished(_) => {
            // Sources stay inside the read-only tool result, not the PC UI snapshot.
            status.search_state="completed".into(); status.search_error.clear();
        },
        crate::web_search::Event::Failed(error) => {
            clear_search(status); status.search_state="failed".into(); status.search_error=error;
        },
    }
}

enum Input {
    Pcm(Vec<u8>, Option<bool>),
}

struct Session {
    id: String,
    board_device_id: String,
    cancelled: AtomicBool,
    duplex: AtomicBool,
    capture_armed: AtomicBool,
    capture_ready: AtomicBool,
    stop_notify: tokio::sync::Notify,
    input: mpsc::Sender<Input>,
    status: Mutex<RealtimeChatStatus>,
    completed_playback: Mutex<String>,
    pending_persona: Mutex<Option<RealtimeChatStartInput>>,
    playback_volume: AtomicU32,
    target_agent_id: String,
    target_session_id: String,
    target_unconfirmed: bool,
}

impl Session {
    fn playback_volume(&self) -> f32 {
        f32::from_bits(self.playback_volume.load(Ordering::Acquire))
    }

    fn set_playback_volume(&self, voice: &VoiceSpec) {
        self.playback_volume.store(voice.volume().to_bits(), Ordering::Release);
    }
}

// Apply once, at the last PC playback boundary, after frame assembly and pacing.
// Raw queued TTS can then use a newly saved gain, even during a long sentence.
fn playback_pcm(session: &Session, raw: &[u8]) -> (Vec<u8>, f32) {
    let gain = session.playback_volume();
    let mut pcm = raw.to_vec();
    scale_pcm16(&mut pcm, gain);
    (pcm, gain)
}

fn active_slot() -> &'static Mutex<Option<Arc<Session>>> {
    static SLOT: OnceLock<Mutex<Option<Arc<Session>>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn active_session() -> Option<Arc<Session>> {
    active_slot().lock().ok().and_then(|slot| slot.clone())
        .filter(|session| session.status.lock().map(|s| s.active).unwrap_or(false))
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub fn status() -> RealtimeChatStatus {
    match active_slot().lock().ok().and_then(|slot| slot.clone()) {
        Some(session) => session.status.lock().map(|s| s.clone()).unwrap_or_default(),
        None => RealtimeChatStatus::default(),
    }
}

fn publish(app: &AppHandle, session: &Session, update: impl FnOnce(&mut RealtimeChatStatus)) {
    let snapshot = match session.status.lock() {
        Ok(mut status) => {
            let previous = status.state.clone();
            update(&mut status);
            if status.state != previous {
                diagnostics::record(&session.id, "state", json!({"state": status.state, "turn": status.turns}));
            }
            status.clone()
        }
        Err(_) => return,
    };
    let _ = app.emit(STATE_EVENT, &snapshot);
}

fn send_board(usb: &UsbSerialManager, board: &str, topic: &str, payload: Value) {
    if let Err(error) = usb.send_to_board(board, topic, &payload) {
        eprintln!("[realtime-chat] send {topic} failed: {error}");
    }
}

async fn begin_capture(usb: &UsbSerialManager, session: &Session, duplex: bool) -> Result<(), String> {
    session.capture_ready.store(false, Ordering::SeqCst);
    session.capture_armed.store(true, Ordering::SeqCst);
    usb.send_to_board(&session.board_device_id, "audio/conversation",
        &json!({"enabled":true,"halfDuplex": !duplex,"sessionId":session.id}))?;
    // Do not start the greeting speaker/reference clock until the DSP owner has
    // rebuilt its filters. Old firmware confirms through conversation status.
    timeout(Duration::from_secs(3), async {
        loop {
            if session.cancelled.load(Ordering::SeqCst) {
                let error=session.status.lock().map(|s|s.error.clone()).unwrap_or_default();
                return Err(if error.is_empty() {"设备收音启动已停止".into()} else {error});
            }
            if session.capture_ready.load(Ordering::SeqCst) { return Ok(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.map_err(|_| "设备收音启动超时，请重新进入；若持续出现，请更新固件并导出实时对话日志".to_string())?
}

fn capture_payload_matches(session: &Session, payload: &Value) -> bool {
    session.capture_armed.load(Ordering::SeqCst)
        && payload.get("boardDeviceId").and_then(Value::as_str)
            .map(|id| id == session.board_device_id).unwrap_or(true)
        && payload.get("conversationSessionId").and_then(Value::as_str)
            .map(|id| id == session.id).unwrap_or(true)
}

fn aec_unavailable(session: &Session, payload: &Value) {
    let reason = match payload["aecReason"].as_str().unwrap_or("") {
        "init_failed" => "init_failed", "stream_reset_failed" => "stream_reset_failed",
        "reference_overflow" => "reference_overflow", "processed_queue_overflow" => "processed_queue_overflow",
        "dsp_too_slow" => "dsp_too_slow", _ => "unprocessed_audio",
    };
    diagnostics::record(&session.id, "aec_fallback", json!({"ok":false,"reason":reason}));
    let detail = match reason {
        "init_failed" | "stream_reset_failed" => "音频处理器初始化失败",
        "reference_overflow" => "回声参考缓冲溢出",
        "processed_queue_overflow" => "收音处理缓冲溢出",
        "dsp_too_slow" => "音频处理持续超时",
        _ => "未收到有效的消回声音频",
    };
    if let Ok(mut s) = session.status.lock() {
        s.error = format!("设备回声消除不可用（{detail}），已停止双向对话。请重新进入以恢复；若持续出现，请更新固件并导出实时对话日志");
    }
    request_stop(session, "aec_unavailable");
}

fn hud(usb: &UsbSerialManager, board: &str, session_id: &str, state: &str, name: &str, text: &str) {
    send_board(
        usb,
        board,
        "ui/conversation",
        json!({ "sessionId": session_id, "state": state, "name": name, "text": text }),
    );
}

fn user_caption(usb: &UsbSerialManager, board: &str, id: &str, name: &str, text: &str, final_text: bool) {
    send_board(usb, board, "ui/conversation", json!({"sessionId":id,"state":"listening",
        "name":name,"text":text,"role":"user","final":final_text}));
}

/// Called from the USB message dispatcher: consume `audio/*` topics while a session is live.
/// Returns true when the message belongs to realtime chat (so push-to-talk logic must skip it).
pub fn route_device_audio(topic: &str, payload: &Value) -> bool {
    if let Some(session) = active_session() {
        if payload.get("boardDeviceId").and_then(Value::as_str)
            .is_some_and(|id| id != session.board_device_id) { return false; }
    }
    if topic == "audio/diagnostic" {
        if payload.get("event").and_then(Value::as_str) == Some("playback_completed") {
            if let (Some(session), Some(id)) = (active_session(), payload.get("sessionId").and_then(Value::as_str)) {
                if id.starts_with(&format!("{}-", session.id)) {
                    if let Ok(mut completed) = session.completed_playback.lock() { *completed = id.to_owned(); }
                }
            }
        }
        let id = active_session().map(|s| s.id.clone()).unwrap_or_default();
        // Device metadata is allowlisted; never log the full audio payload.
        diagnostics::record(&id, "device", json!({"stage": payload.get("event"), "bytes": payload.get("bytes"),
            "bufferedMs": payload.get("bufferedMs"), "uptimeMs": payload.get("uptimeMs"), "seq":payload.get("seq"), "ok":payload.get("ok"), "firmware":payload.get("firmware"),
            "reason":payload.get("aecReason"), "dspFrames":payload.get("dspFrames"), "dspLastUs":payload.get("dspLastUs"), "dspMaxUs":payload.get("dspMaxUs"),
            "dspAecUs":payload.get("dspAecUs"), "dspNsUs":payload.get("dspNsUs"), "dspVadUs":payload.get("dspVadUs"), "aecReferenceSamples":payload.get("aecReferenceSamples"),
            "internalFreeBytes":payload.get("internalFreeBytes"), "internalLargestBytes":payload.get("internalLargestBytes"),
            "captureTxLastUs":payload.get("captureTxLastUs"), "captureTxMaxUs":payload.get("captureTxMaxUs"), "captureStackMinBytes":payload.get("captureStackMinBytes")}));
        return true;
    }
    let Some(session) = active_session() else { return false };
    route_session_audio(&session, topic, payload)
}

fn route_session_audio(session: &Session, topic: &str, payload: &Value) -> bool {
    if payload.get("boardDeviceId").and_then(Value::as_str)
        .is_some_and(|id| id != session.board_device_id) { return false; }
    if topic == "protocol/ack" && session.capture_armed.load(Ordering::SeqCst)
        && payload.get("ok").and_then(Value::as_bool) == Some(false)
        && payload.get("requestTopic").and_then(Value::as_str).map(|s| s.starts_with("audio/")).unwrap_or(false) {
        let stage = match payload["requestTopic"].as_str().unwrap_or("") {
            "audio/play_chunk" => "audio_play_chunk", "audio/play_begin" => "audio_play_begin",
            "audio/play_end" => "audio_play_end", "audio/play_subtitle" => "audio_play_subtitle",
            "audio/conversation" => "audio_conversation", _ => "audio_other",
        };
        let reason = match payload["code"].as_str().unwrap_or("") {
            "audio_chunk_rejected" => "audio_chunk_rejected", "audio_chunk_invalid" => "audio_chunk_invalid",
            "audio_playback_failed" => "audio_playback_failed", "invalid_subtitle" => "invalid_subtitle",
            _ => "audio_command_rejected",
        };
        diagnostics::record(&session.id, "protocol_rejected", json!({"stage":stage,"reason":reason,"ok":false}));
        // A single full/bad chunk is recoverable: dropping it leaves a short
        // gap, whereas aborting the whole turn makes the device look dead.
        // Invalid payloads still fail closed because they indicate a broken
        // protocol stream rather than transient backpressure.
        if reason == "audio_chunk_rejected" {
            diagnostics::record(&session.id, "audio_chunk_dropped", json!({"ok":false,"reason":reason}));
            return true;
        }
        if let Ok(mut s) = session.status.lock() {
            s.error = if reason == "audio_chunk_invalid" {
                "设备未能接收音频数据，已停止播放；请提供实时对话日志排查 USB 传输".into()
            } else { "设备拒绝音频指令，请检查固件及设备音频状态".into() };
        }
        request_stop(&session, "device_protocol_error");
        return true;
    }
    if !topic.starts_with("audio/") { return false; }
    if session.cancelled.load(Ordering::SeqCst) {
        return topic.starts_with("audio/");
    }
    // Ignore pre-entry PTT frames, previous capture timelines and stale status.
    // New firmware echoes the control session independently of capture sessionId.
    if !capture_payload_matches(&session, payload) { return true; }
    if topic == "audio/status" {
        if payload["conversation"] != true || payload["audioAecPreparing"] == true { return true; }
        if session.duplex.load(Ordering::SeqCst) && payload["audioAec"] == false {
            aec_unavailable(&session, payload);
        } else if !session.duplex.load(Ordering::SeqCst) || payload["audioAec"] == true {
            session.capture_ready.store(true, Ordering::SeqCst);
        }
        return true;
    }
    if topic == "audio/chunk" {
        let processed = payload["aec"] == true && payload["vadSpeech"].is_boolean();
        let duplex = session.duplex.load(Ordering::SeqCst);
        if duplex && !processed {
            // Legacy firmware lacks timeline IDs: ignore its queued PTT frames
            // until explicit conversation readiness, bounded by begin_capture.
            if !session.capture_ready.load(Ordering::SeqCst) && payload.get("conversationSessionId").is_none() { return true; }
            // Never route raw speaker echo into ASR/tool calls after an AFE failure.
            aec_unavailable(&session, payload);
            return true;
        }
        if processed { session.capture_ready.store(true, Ordering::SeqCst); }
        if !duplex && !session.status.lock().map(|s| s.state == "listening").unwrap_or(false) { return true; }
        if let Some(data) = payload.get("data").and_then(Value::as_str) {
            if let Ok(pcm) = BASE64.decode(data) {
                if pcm.len() != FRAME_BYTES { return true; }
                if session.input.try_send(Input::Pcm(pcm, if duplex {payload["vadSpeech"].as_bool()} else {None})).is_err() {
                    if let Ok(mut s) = session.status.lock() { s.error = "收音处理队列繁忙，请重新开始对话".into(); }
                    request_stop(&session, "capture_overflow");
                }
            }
        }
        return true;
    }
    if topic == "audio/error" {
        let message = payload.get("error").or_else(|| payload.get("message")).and_then(Value::as_str).unwrap_or("device audio error");
        if let Ok(mut s) = session.status.lock() { s.error = format!("设备音频错误：{message}"); }
        request_stop(&session, "device_audio_error");
        return true;
    }
    // audio/begin, audio/end, audio/status: informational while the conversation runs
    true
}

/// Called from the USB message dispatcher for `input/event`. Handles the `realtime_chat` action:
/// a live session stops; otherwise the UI is asked to start one for the current appearance.
fn realtime_entry_context(context: &str) -> bool {
    matches!(context, "main" | "app" | "components")
}

pub fn handle_input_event(app: &AppHandle, payload: &Value) -> bool {
    if let Some(session) = active_session() {
        if payload.get("boardDeviceId").and_then(Value::as_str) == Some(session.board_device_id.as_str()) {
            let action = payload.get("action").and_then(Value::as_str).unwrap_or_default();
            if action == ACTION_ID {
                request_stop(&session, "key");
            } else if action == "dsh_interrupt" {
                // The device's configured DSH interrupt must reach the DSH task
                // even while realtime chat owns every other physical key.
                crate::handle_dsh_interrupt_action(app, payload);
            }
            return true; // No Agent/session navigation or submission while realtime owns this board.
        }
    }
    if payload.get("action").and_then(Value::as_str) != Some(ACTION_ID) {
        return false;
    }
    let board = payload.get("boardDeviceId").and_then(Value::as_str).unwrap_or("").to_string();
    let context = payload.get("context").and_then(Value::as_str).unwrap_or("main").to_string();
    if let Some(session) = active_session() {
        request_stop(&session, "key");
        return true;
    }
    if !realtime_entry_context(&context) {
        // Global realtime input works on all supported device pages. Unknown
        // contexts still fail closed, rather than dispatching an Agent action.
        return true;
    }
    let _ = app.emit(REQUEST_EVENT, json!({ "boardDeviceId": board, "context": context, "at": now_ms() }));
    true
}

pub fn stop(reason: &str) -> RealtimeChatStatus {
    if let Some(session) = active_session() {
        request_stop(&session, reason);
        return session.status.lock().map(|s| s.clone()).unwrap_or_default();
    }
    RealtimeChatStatus::default()
}

fn request_stop(session: &Session, reason: &str) {
    crate::smart_home::cancel_voice_session(&session.id);
    if let Ok(mut s) = session.status.lock() {
        if s.reason == "asr_timeout" { s.error.clear(); }
        s.reason = reason.to_string();
    }
    session.cancelled.store(true, Ordering::SeqCst);
    session.stop_notify.notify_one();
}

fn start_guard() -> &'static Mutex<()> {
    static START_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    START_GUARD.get_or_init(|| Mutex::new(()))
}

pub fn start(app: &AppHandle, mut input: RealtimeChatStartInput) -> Result<RealtimeChatStatus, String> {
    let _guard = start_guard().lock().map_err(|_| "实时对话启动锁异常")?;
    let board = input.board_device_id.trim().to_string();
    if board.is_empty() {
        return Err("缺少设备 id".to_string());
    }
    if input.system_prompt.trim().is_empty() {
        return Err("这个形象还没有人设，先在画廊里点「人设与声音」设置".to_string());
    }
    let settings = voice_chat_settings::status()?;
    if !settings.llm_configured {
        return Err("对话大模型未配置，请在「API 配置」填写大模型 API Key".to_string());
    }
    if !settings.tts_configured {
        return Err("豆包 TTS 未配置，请在「API 配置」填写语音合成 API Key".to_string());
    }
    if !crate::volcengine_asr::settings_status()?.configured {
        return Err("语音识别未配置，请先在 API 配置填写 ASR Key".into());
    }
    let usb = app.state::<UsbSerialManager>().inner().clone();
    let usb_status = usb.status();
    if !usb_status.connected || usb_status.board_device_id != board {
        return Err("设备未通过 USB 连接，实时对话需要设备在线".to_string());
    }
    if active_session().is_some() { return Err("已有实时对话，请先结束后再开始".into()); }
    crate::component_notes::stop_recording_all();
    crate::component_audio::stop(None);
    // The native global setting is authoritative; ignore legacy per-pet gain
    // and stale frontend snapshots, including on hardware-button entry.
    input.voice.volume = voice_chat_settings::realtime_volume()?;

    let (tx, rx) = mpsc::channel(128);
    let session_id = format!("rtc-{}", now_ms());
    let session = Arc::new(Session {
        id: session_id.clone(),
        board_device_id: board.clone(),
        cancelled: AtomicBool::new(false),
        duplex: AtomicBool::new(usb_status.capabilities["features"]["audioAecRecovery"] == true
            || ["audioAec","audioVad","audioFullDuplex"].iter().all(|key| usb_status.capabilities["features"][*key] == true)),
        capture_armed: AtomicBool::new(false),
        capture_ready: AtomicBool::new(false),
        stop_notify: tokio::sync::Notify::new(),
        input: tx,
        completed_playback: Mutex::new(String::new()),
        pending_persona: Mutex::new(None),
        playback_volume: AtomicU32::new(input.voice.volume().to_bits()),
        target_agent_id: input.target_agent_id.trim().to_string(),
        target_session_id: input.target_session_id.trim().to_string(),
        target_unconfirmed: input.target_unconfirmed,
        status: Mutex::new(RealtimeChatStatus {
            active: true,
            state: "preparing".to_string(),
            session_id: session_id.clone(),
            board_device_id: board.clone(),
            appearance_id: input.appearance_id.clone(),
            display_name: input.display_name.clone(),
            started_at_ms: now_ms(),
            ..RealtimeChatStatus::default()
        }),
    });
    if let Ok(mut slot) = active_slot().lock() {
        *slot = Some(session.clone());
    }
    let snapshot = session.status.lock().map(|s| s.clone()).unwrap_or_default();
    let _ = app.emit(STATE_EVENT, &snapshot);

    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        diagnostics::record(&session.id, "started", json!({"volume":session.playback_volume()}));
        tokio::select! {
            biased;
            _ = session.stop_notify.notified() => {
                let (reason, error) = session.status.lock().map(|s| (s.reason.clone(), s.error.clone())).unwrap_or_default();
                finish(&app_clone, &usb, &session, &reason, &error).await;
            }
            _ = run_session(app_clone.clone(), usb.clone(), session.clone(), input, rx) => {}
        }
    });
    Ok(snapshot)
}

// ---- energy VAD ---------------------------------------------------------------------------

struct Vad {
    noise: f32,
    processed_noise: f32,
    speech_run: usize,
    silence_run: usize,
    in_speech: bool,
    preroll: Vec<Vec<u8>>,
    speech_started: Option<Instant>,
    soft_gap: usize,
    metrics: VadMetrics,
    diagnostic_session: String,
    reply_tail_until: Option<Instant>,
    session: Option<Arc<Session>>,
    previous_user: String,
    turn_started: Option<Instant>,
}

#[derive(Default)]
struct VadMetrics {
    frames: usize,
    hints: usize,
    rejected: usize,
    peak: f32,
    starts: usize,
    reply_frames: usize,
}

#[derive(Default)]
struct AsrCaptureStats { frames: usize, peak: f32 }
impl AsrCaptureStats {
    fn observe(&mut self, frame: &[u8]) {
        self.frames += 1;
        self.peak = self.peak.max(Vad::rms(frame));
    }
    fn record(&self, session: &str, turn: u32) {
        diagnostics::record(session, "asr_audio_submitted", json!({"turn":turn,"frames":self.frames,
            "bytes":self.frames*FRAME_BYTES,"peakRms":self.peak.round()}));
    }
}

#[derive(Clone, Copy)]
enum VadContext { Listening, Reply }

enum VadStep {
    Idle,
    Start(Vec<Vec<u8>>),
    Speech,
    End,
}

impl Vad {
    fn new() -> Self {
        Self { noise: 200.0, processed_noise: 16.0, speech_run: 0, silence_run: 0, in_speech: false, preroll: Vec::new(), speech_started: None, soft_gap: 0, metrics: VadMetrics::default(), diagnostic_session: String::new(), reply_tail_until: None, session: None, previous_user: String::new(), turn_started: None }
    }

    fn rms(frame: &[u8]) -> f32 {
        let mut acc = 0f64;
        let mut n = 0usize;
        for pair in frame.chunks_exact(2) {
            let v = i16::from_le_bytes([pair[0], pair[1]]) as f64;
            acc += v * v;
            n += 1;
        }
        if n == 0 { 0.0 } else { (acc / n as f64).sqrt() as f32 }
    }

    fn step(&mut self, frame: &[u8]) -> VadStep {
        self.step_with_hint(frame, None)
    }

    fn step_with_hint(&mut self, frame: &[u8], speech: Option<bool>) -> VadStep {
        self.step_in_context(frame, speech, VadContext::Reply)
    }

    fn step_in_context(&mut self, frame: &[u8], speech: Option<bool>, context: VadContext) -> VadStep {
        let context = if self.reply_tail_until.is_some_and(|until| Instant::now() < until) { VadContext::Reply } else { context };
        let level = Self::rms(frame);
        let floor = if matches!(context, VadContext::Listening) { LISTENING_VAD_ABS_MIN } else { PROCESSED_VAD_ABS_MIN };
        if !self.in_speech {
            // slow-tracking noise floor, only while nobody is talking
            self.noise = if level < self.noise { self.noise * 0.9 + level * 0.1 } else { self.noise * 0.995 + level * 0.005 };
        }
        let threshold = (self.noise * VAD_NOISE_RATIO).max(VAD_ABS_MIN);
        let voiced = match speech {
            Some(hint) => {
                if !self.in_speech && !hint {
                    self.processed_noise = if level < self.processed_noise {
                        self.processed_noise * 0.9 + level * 0.1
                    } else { self.processed_noise * 0.995 + level * 0.005 };
                }
                let credible = if self.in_speech {
                    (self.processed_noise * 1.4).max(LISTENING_VAD_ABS_MIN / 2.0)
                } else { (self.processed_noise * VAD_NOISE_RATIO).max(floor) };
                hint && level >= credible
            }
            None => level >= threshold,
        };
        self.metrics.frames += 1;
        self.metrics.reply_frames += usize::from(matches!(context, VadContext::Reply));
        self.metrics.hints += usize::from(speech == Some(true));
        self.metrics.rejected += usize::from(speech == Some(true) && !voiced);
        self.metrics.peak = self.metrics.peak.max(level);
        self.log_metrics();
        if !self.in_speech {
            self.preroll.push(frame.to_vec());
            if self.preroll.len() > PREROLL_FRAMES {
                self.preroll.remove(0);
            }
            if voiced {
                self.speech_run += 1;
                let start_frames = if speech.is_none() { SPEECH_START_FRAMES }
                    else if matches!(context, VadContext::Listening) { LISTENING_SPEECH_START_FRAMES }
                    else { PROCESSED_SPEECH_START_FRAMES };
                if self.speech_run >= start_frames {
                    self.metrics.starts += 1;
                    self.in_speech = true;
                    self.silence_run = 0;
                    self.speech_started = Some(Instant::now());
                    let preroll = std::mem::take(&mut self.preroll);
                    return VadStep::Start(preroll);
                }
            } else {
                // Keep short, speech-labelled energy dips (consonants) without
                // counting them as evidence. A false device VAD immediately
                // breaks the run, preserving onset-echo rejection during playback.
                self.soft_gap += 1;
                if speech != Some(true) || self.soft_gap > 2 {
                    self.speech_run = 0;
                    self.soft_gap = 0;
                }
            }
            return VadStep::Idle;
        }
        if voiced {
            self.silence_run = 0;
        } else {
            self.silence_run += 1;
        }
        let too_long = self
            .speech_started
            .map(|t| t.elapsed() >= Duration::from_millis(MAX_UTTERANCE_MS))
            .unwrap_or(false);
        if self.silence_run >= SPEECH_END_FRAMES || too_long {
            self.in_speech = false;
            self.speech_run = 0;
            self.soft_gap = 0;
            self.silence_run = 0;
            self.speech_started = None;
            return VadStep::End;
        }
        VadStep::Speech
    }

    fn reset(&mut self) {
        self.in_speech = false;
        self.speech_run = 0;
        self.soft_gap = 0;
        self.silence_run = 0;
        self.preroll.clear();
        self.speech_started = None;
    }

    fn log_metrics(&mut self) {
        if self.metrics.frames < 250 { return; }
        let m = std::mem::take(&mut self.metrics);
        if self.diagnostic_session.is_empty() { return; }
        diagnostics::record(&self.diagnostic_session, "vad_window", json!({"frames":m.frames,"hintFrames":m.hints,
            "rejectedFrames":m.rejected,"peakRms":m.peak.round(),"count":m.starts,"replyFrames":m.reply_frames}));
    }
}

// ---- no-transcript short-command evidence ---------------------------------------------------

const PERIODIC_MIN_HZ: f32 = 80.0;
const PERIODIC_MAX_HZ: f32 = 400.0;
const PERIODIC_MIN_CORRELATION: f32 = 0.55;
// A vowel has a low zero-crossing rate; clicks, claps, coughs and broadband
// hiss are dominated by high-frequency crossings and must not count as voiced.
const PERIODIC_MAX_ZCR: f32 = 0.20;

/// Near-end voiced-speech evidence for the no-transcript barge fallback.
///
/// This is deliberately only a *voice* check, never a command-word check: the
/// cloud Final is empty, so the fallback relies on the board's AEC/VAD hint,
/// the PC energy VAD, short duration and pitch periodicity. Playback echo and
/// arbitrary noise fail at the AEC/VAD gate or here (aperiodic, high ZCR).
fn periodic_voice_frame(frame: &[u8]) -> bool {
    let samples: Vec<f32> = frame
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]) as f32)
        .collect();
    if samples.len() < 64 || Vad::rms(frame) < PROCESSED_VAD_ABS_MIN {
        return false;
    }
    let mean = samples.iter().map(|v| *v as f64).sum::<f64>() / samples.len() as f64;
    let centered: Vec<f32> = samples.iter().map(|v| (*v as f64 - mean) as f32).collect();
    if centered.iter().map(|v| v * v).sum::<f32>() <= f32::EPSILON {
        return false;
    }
    let min_lag = (16_000.0 / PERIODIC_MAX_HZ).ceil() as usize;
    let max_lag = ((16_000.0 / PERIODIC_MIN_HZ).floor() as usize).min(centered.len() - 2);
    let mut best = 0.0f32;
    for lag in min_lag..=max_lag {
        let (mut numerator, mut left, mut right) = (0.0f32, 0.0f32, 0.0f32);
        for i in 0..(centered.len() - lag) {
            numerator += centered[i] * centered[i + lag];
            left += centered[i] * centered[i];
            right += centered[i + lag] * centered[i + lag];
        }
        let denominator = (left * right).sqrt();
        if denominator > f32::EPSILON {
            best = best.max(numerator / denominator);
        }
    }
    if best < PERIODIC_MIN_CORRELATION {
        return false;
    }
    let crossings = centered.windows(2).filter(|pair| (pair[0] > 0.0) != (pair[1] > 0.0)).count();
    crossings as f32 / (centered.len() - 1) as f32 <= PERIODIC_MAX_ZCR
}

// ---- board playback sender ------------------------------------------------------------------

enum PlayerCmd {
    Begin(String),
    Subtitle(String),
    Pcm(Vec<u8>),
    End(std_mpsc::Sender<()>),
    Flush,
}

#[derive(Clone)]
struct Player {
    tx: std_mpsc::Sender<(u64, PlayerCmd)>,
    epoch: Arc<AtomicU64>,
}
impl Player {
    fn epoch(&self) -> u64 { self.epoch.load(Ordering::SeqCst) }
    fn send_at(&self, epoch: u64, cmd: PlayerCmd) -> Result<(), ()> {
        self.tx.send((epoch, cmd)).map_err(|_| ())
    }
    fn send(&self, cmd: PlayerCmd) -> Result<(), ()> {
        let epoch = if matches!(&cmd, PlayerCmd::Flush) {
            self.epoch.fetch_add(1, Ordering::SeqCst) + 1
        } else { self.epoch.load(Ordering::SeqCst) };
        self.tx.send((epoch, cmd)).map_err(|_| ())
    }
}

/// Bounded recovery for one transient board audio write stall.
///
/// The diagnosed long-reply stall (docs/p4-usb-cdc-long-reply-diagnosis-2026-11-03.md)
/// is a few-hundred-millisecond macOS tty write timeout while the board and the
/// app stay alive. A late 100 ms chunk is already useless, so drop it and keep
/// the reply going; permanent transport loss or a sustained stall still ends
/// the session (fail closed). No unbounded retry lives here: each frame already
/// has the transport layer's bounded 8 x 1.5 s recovery.
#[derive(Debug, Default, PartialEq, Eq)]
struct PlaybackWriteRecovery {
    consecutive_failures: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum PlaybackWriteAction {
    DropChunk,
    Stop,
}

impl PlaybackWriteRecovery {
    fn on_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn on_failure(&mut self, error: &str) -> PlaybackWriteAction {
        if !playback_write_error_is_transient(error) {
            return PlaybackWriteAction::Stop;
        }
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures > AUDIO_WRITE_MAX_CONSECUTIVE_FAILURES {
            PlaybackWriteAction::Stop
        } else {
            PlaybackWriteAction::DropChunk
        }
    }
}

/// Only queue/tty timeouts are worth a bounded drop. Broken pipe, an unplugged
/// board, unknown IO errors and non-serial send failures all fail closed.
fn playback_write_error_is_transient(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    let transient = ["timeout", "would_block", "interrupted"]
        .iter()
        .any(|marker| error.contains(marker));
    let permanent = [
        "broken_pipe",
        "not_connected",
        "permission_denied",
        "write_zero",
        "write_progress_unknown",
        "io_error",
        "not connected",
        "connection lost",
    ]
    .iter()
    .any(|marker| error.contains(marker));
    transient && !permanent
}

/// Dedicated thread: packs PCM into 100 ms `audio/play_chunk` frames and paces them so the board
/// never holds more than ~1.2 s of audio ahead of real time.
fn spawn_player(usb: UsbSerialManager, board: String, session: Arc<Session>) -> Player {
    let subtitles = usb.status().capabilities.pointer("/features/conversationSubtitles").and_then(Value::as_bool) == Some(true);
    let await_device = subtitles && active_session().map(|s| s.id == session.id).unwrap_or(false);
    let (tx, rx) = std_mpsc::channel::<(u64, PlayerCmd)>();
    let epoch = Arc::new(AtomicU64::new(0));
    let player = Player { tx, epoch: epoch.clone() };
    thread::spawn(move || {
        #[cfg(target_os = "macos")]
        let _audio_activity = {
            let activity = crate::realtime_audio_macos::AudioActivity::begin_on_player_thread();
            diagnostics::record(&session.id, "playback_scheduling", json!({
                "platform":"macos", "activity":"latency_critical", "qos":"user_initiated",
                "qosApplied":activity.qos_error == 0, "qosError":activity.qos_error
            }));
            activity
        };
        let mut current_epoch = 0;
        let mut turn_id = String::new();
        let mut pending: Vec<u8> = Vec::new();
        let mut seq: u32 = 0;
        let mut begin_at: Option<Instant> = None;
        let mut sent_bytes: u64 = 0;
        let mut write_recovery = PlaybackWriteRecovery::default();
        let flush_chunk = |usb: &UsbSerialManager, turn_id: &str, seq: &mut u32, chunk: &[u8], begin_at: &mut Option<Instant>, sent_bytes: &mut u64, expected: u64, recovery: &mut PlaybackWriteRecovery| {
            if chunk.is_empty() {
                return;
            }
            let started = *begin_at.get_or_insert_with(Instant::now);
            let lead_ms = (*sent_bytes * 1000 / 32_000) as i64 - started.elapsed().as_millis() as i64;
            let wait = Duration::from_millis(lead_ms.saturating_sub(PLAYBACK_LEAD_MS as i64).max(0) as u64);
            let until = Instant::now() + wait;
            while Instant::now() < until && !session.cancelled.load(Ordering::SeqCst) && epoch.load(Ordering::SeqCst) == expected { thread::sleep(Duration::from_millis(10)); }
            if session.cancelled.load(Ordering::SeqCst) || epoch.load(Ordering::SeqCst) != expected { return; }
            let write_started = Instant::now();
            let (pcm, gain) = playback_pcm(&session, chunk);
            let result = usb.send_to_board(
                &board,
                "audio/play_chunk",
                &json!({ "sessionId": turn_id, "seq": *seq, "bytes": pcm.len(), "data": BASE64.encode(&pcm) }),
            );
            let elapsed_ms = write_started.elapsed().as_millis();
            // Metadata only: no PCM, transcript, key, port or personal path.
            if *seq < 3 || *seq % 10 == 0 || elapsed_ms >= 80 || result.is_err() {
                diagnostics::record(&session.id, "playback_tx", json!({"seq":*seq,
                    "bytes":chunk.len(),"elapsedMs":elapsed_ms,"ok":result.is_ok(),"volume":gain,
                    "errorKind":result.as_ref().err().map(|error|diagnostics::error_kind(error))}));
            }
            if let Err(error) = result {
                eprintln!("[realtime-chat] device audio transport write failed: {error}");
                match recovery.on_failure(&error) {
                    PlaybackWriteAction::DropChunk => {
                        // Bounded recovery: this late chunk is already useless, so
                        // drop it and resume the stream on the next one instead of
                        // ending the reply. seq/sent_bytes stay put because the
                        // dropped audio was never played.
                        diagnostics::record(&session.id, "playback_write_gap", json!({
                            "seq":*seq, "bytes":chunk.len(), "elapsedMs":elapsed_ms,
                            "consecutive":recovery.consecutive_failures,
                            "errorKind":diagnostics::error_kind(&error), "ok":false,
                        }));
                        return;
                    }
                    PlaybackWriteAction::Stop => {
                        if let Ok(mut status) = session.status.lock() { status.error = "设备音频传输失败，请检查 USB 连接后重试".into(); }
                        request_stop(&session, "playback_write_failed");
                        return;
                    }
                }
            }
            recovery.on_success();
            *seq += 1;
            *sent_bytes += chunk.len() as u64;
        };
        while let Ok((expected, cmd)) = rx.recv() {
            if session.cancelled.load(Ordering::SeqCst) { break; }
            if expected != epoch.load(Ordering::SeqCst) { continue; }
            if current_epoch != expected {
                pending.clear(); begin_at = None; sent_bytes = 0;
                write_recovery = PlaybackWriteRecovery::default();
                send_board(&usb, &board, "audio/play_flush", json!({"sessionId":turn_id}));
                current_epoch = expected;
            }
            match cmd {
                PlayerCmd::Begin(id) => {
                    if let Ok(mut completed) = session.completed_playback.lock() { completed.clear(); }
                    turn_id = id;
                    pending.clear();
                    seq = 0;
                    begin_at = None;
                    sent_bytes = 0;
                    write_recovery = PlaybackWriteRecovery::default();
                    send_board(
                        &usb,
                        &board,
                        "audio/play_begin",
                        json!({ "sessionId": turn_id, "sampleRate": 16000, "channels": 1, "format": "pcm_s16le" }),
                    );
                }
                PlayerCmd::Subtitle(text) => {
                    if subtitles {
                        send_board(&usb, &board, "audio/play_subtitle", json!({"sessionId":turn_id,
                            "offsetBytes":sent_bytes + pending.len() as u64,"text":text}));
                    }
                }
                PlayerCmd::Pcm(bytes) => {
                    pending.extend_from_slice(&bytes);
                    while pending.len() >= PLAY_CHUNK_BYTES {
                        let chunk: Vec<u8> = pending.drain(..PLAY_CHUNK_BYTES).collect();
                        flush_chunk(&usb, &turn_id, &mut seq, &chunk, &mut begin_at, &mut sent_bytes, expected, &mut write_recovery);
                        if session.cancelled.load(Ordering::SeqCst) || epoch.load(Ordering::SeqCst) != expected { pending.clear(); break; }
                    }
                }
                PlayerCmd::End(done) => {
                    let chunk = std::mem::take(&mut pending);
                    flush_chunk(&usb, &turn_id, &mut seq, &chunk, &mut begin_at, &mut sent_bytes, expected, &mut write_recovery);
                    if session.cancelled.load(Ordering::SeqCst) || epoch.load(Ordering::SeqCst) != expected { continue; }
                    send_board(&usb, &board, "audio/play_end", json!({ "sessionId": turn_id, "bytes": sent_bytes }));
                    if let Some(started) = begin_at {
                        let total = Duration::from_millis(sent_bytes * 1000 / 32_000);
                        let elapsed = started.elapsed();
                        if total > elapsed {
                            let until = Instant::now() + total - elapsed;
                            while Instant::now() < until && !session.cancelled.load(Ordering::SeqCst) && epoch.load(Ordering::SeqCst) == expected { thread::sleep(Duration::from_millis(10)); }
                        }
                    }
                    if await_device && sent_bytes > 0 {
                        // Do not clear the spoken caption or resume ASR while buffered audio is still playing.
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while !session.cancelled.load(Ordering::SeqCst)
                            && epoch.load(Ordering::SeqCst) == expected
                            && !session.completed_playback.lock().map(|id| *id == turn_id).unwrap_or(false) {
                            if Instant::now() >= deadline {
                                if let Ok(mut status) = session.status.lock() { status.error = "设备播放完成确认超时，请检查 USB 连接".into(); }
                                request_stop(&session, "playback_timeout");
                                break;
                            }
                            thread::sleep(Duration::from_millis(10));
                        }
                    }
                    let _ = done.send(());
                    begin_at = None;
                    sent_bytes = 0;
                }
                PlayerCmd::Flush => {
                    pending.clear();
                    send_board(&usb, &board, "audio/play_flush", json!({ "sessionId": turn_id }));
                    begin_at = None;
                    sent_bytes = 0;
                    write_recovery = PlaybackWriteRecovery::default();
                }
            }
        }
    });
    player
}

// ---- session loop ---------------------------------------------------------------------------

async fn run_session(
    app: AppHandle,
    usb: UsbSerialManager,
    session: Arc<Session>,
    mut input: RealtimeChatStartInput,
    mut rx: mpsc::Receiver<Input>,
) {
    let board = session.board_device_id.clone();
    let mut name = input.display_name.clone();
    let voice = input.voice.clone();
    let reason: String;
    let mut history: Vec<ChatTurn> = Vec::new();
    let mut desktop_conversation = crate::desktop_agent::Conversation::default();
    crate::smart_home::prepare_agent_capabilities();

    let llm_cfg = voice_chat_settings::llm_config();
    let tts_cfg = voice_chat_settings::tts_config(&voice.speaker, &voice.clone_speaker_id, voice.speed());
    let (llm_cfg, tts_cfg) = match (llm_cfg, tts_cfg) {
        (Ok(l), Ok(t)) => (l, t),
        (Err(error), _) | (_, Err(error)) => {
            finish(&app, &usb, &session, "config", &error).await;
            return;
        }
    };
    let mut tts = DoubaoTtsClient::new(tts_cfg);
    let player = spawn_player(usb.clone(), board.clone(), session.clone());
    let duplex = session.duplex.load(Ordering::SeqCst);
    let mut vad = Vad::new();
    vad.diagnostic_session = session.id.clone();
    vad.session = Some(session.clone());
    let mut pending_speech = None;
    let mut deferred_audio = VecDeque::new();
    diagnostics::record(&session.id, "audio_mode", json!({"state":if duplex {"full_duplex"} else {"half_duplex"}}));

    hud(&usb, &board, &session.id, "preparing", &name, "准备中");

    // Validate TTS with the actual greeting before enabling continuous microphone capture.
    let greeting = input.greeting.trim().to_string();
    if !greeting.is_empty() {
        let mut greeting_pcm = Vec::new();
        if let Err(error) = tts.synthesize(&greeting, |pcm| greeting_pcm.extend_from_slice(pcm)).await {
            finish(&app, &usb, &session, "tts", &error).await;
            return;
        }
        if duplex {
            if let Err(error) = begin_capture(&usb, &session, true).await {
                finish(&app, &usb, &session, "audio", &error).await; return;
            }
        }
        diagnostics::record(&session.id, "greeting_ready", json!({"bytes":greeting_pcm.len()}));
        publish(&app, &session, |s| { s.state = "speaking".into(); s.reply = greeting.clone(); });
        hud(&usb, &board, &session.id, "speaking", &name, "");
        let _ = player.send(PlayerCmd::Begin(format!("{}-greeting", session.id)));
        let _ = player.send(PlayerCmd::Subtitle(greeting.clone()));
        let _ = player.send(PlayerCmd::Pcm(greeting_pcm));
        match listen_with_verified_barge_in(async {wait_playback(&player).await; Ok(())}, &mut rx, &mut deferred_audio, &mut vad, duplex).await {
            Ok(Some(speech)) if speech.no_transcript_stop => {
                let _ = player.send(PlayerCmd::Flush);
                tts.interrupt();
                diagnostics::record(&session.id,"voice_stop",json!({"turn":0,"chars":0,"reason":"no_transcript"}));
            }
            Ok(frames) => pending_speech = frames,
            Err(error) => {finish(&app, &usb, &session, "audio", &error).await; return;}
        }
        if pending_speech.as_ref().is_some_and(|speech| !speech.reply_completed) { let _ = player.send(PlayerCmd::Flush); }
    }
    if !duplex || greeting.is_empty() {
        if let Err(error) = begin_capture(&usb, &session, duplex).await {
            finish(&app, &usb, &session, "audio", &error).await; return;
        }
    }
    if !duplex { drain_pending(&mut rx); }
    publish(&app, &session, |s| s.state = "listening".into());
    hud(&usb, &board, &session.id, "listening", &name, "");

    let mut recognizer: Option<StreamingSpeechRecognizer> = None;
    let mut capture_stats = AsrCaptureStats::default();
    let mut asr_rx: Option<mpsc::UnboundedReceiver<StreamingSpeechEvent>> = None;
    let mut last_activity = Instant::now();
    let mut turns: u32 = 0;
    let mut latest_asr = String::new();
    let mut displayed_asr = String::new();
    let mut final_asr: Option<String> = None;
    let mut recognizer_terminal = false;
    let mut caption_sent_at = Instant::now();
    let mut desktop_progress = String::new();
    let mut asr_started_at = Instant::now();
    let mut asr_error: Option<(String, &'static str)> = None;
    let mut asr_timeouts: u32 = 0;

    'conversation: loop {
        if session.cancelled.load(Ordering::SeqCst) {
            reason = "cancelled".into();
            break;
        }
        if let Some((error, stage)) = asr_error.take() {
            diagnostics::record(&session.id, "asr_round_failed", json!({
                "turn":turns+1,"stage":stage,"errorKind":diagnostics::error_kind(&error),
                "elapsedMs":asr_started_at.elapsed().as_millis(),"chars":latest_asr.chars().count(),"ok":false
            }));
            if !recoverable_asr_error(&error) {
                finish(&app, &usb, &session, "asr", &error).await;
                return;
            }
            // Abandon this recognizer, including any late results. Do not replay
            // audio or send partial text to the model. USB capture/AEC stay live.
            recognizer.take(); asr_rx.take();
            reset_timed_out_asr(&mut vad, &mut latest_asr, &mut displayed_asr,
                &mut final_asr, &mut recognizer_terminal, &mut deferred_audio);
            asr_timeouts = asr_timeouts.saturating_add(1);
            last_activity = Instant::now();
            publish(&app, &session, |s| {
                s.state="listening".into(); s.transcript.clear();
                s.reason="asr_timeout".into();
                s.error="刚才识别超时，仍在聆听，请再说一次。".into();
            });
            user_caption(&usb, &board, &session.id, &name, "", false);
            hud(&usb, &board, &session.id, "listening", &name, "识别超时，请再说一次");
            diagnostics::record(&session.id, "asr_recovered", json!({"reason":"timeout", "count":asr_timeouts, "ok":true}));
        }
        if let Some(speech) = pending_speech.take() {
            asr_started_at = Instant::now();
            latest_asr = speech.text; displayed_asr.clear();
            recognizer_terminal = speech.terminal;
            final_asr = speech.terminal.then(|| latest_asr.clone());
            last_activity = Instant::now();
            capture_stats = speech.stats;
            recognizer = Some(speech.recognizer); asr_rx = Some(speech.events);
            publish(&app, &session, |s| {s.state="listening".into(); s.transcript = latest_asr.clone(); s.error.clear(); s.reason.clear();});
            user_caption(&usb,&board,&session.id,&name,&latest_asr,false);
            diagnostics::record(&session.id,"asr_started",json!({"turn":turns+1,"reason":"confirmed_barge_in"}));
        }
        match apply_pending_persona(&session, &mut input, &mut tts).await {
            Ok(true) => {
                name = input.display_name.clone();
                publish(&app, &session, |s| s.display_name = name.clone());
                hud(&usb, &board, &session.id, "listening", &name, &latest_asr);
            }
            Ok(false) => {}
            Err(error) => { finish(&app, &usb, &session, "config", &error).await; return; }
        }
        if recognizer.is_none() && !vad.in_speech && vad.speech_run==0 && pending_speech.is_none() && deferred_audio.is_empty() {
            let approval_pending = crate::dsh_approval::device_approval_pending();
            let notification = if approval_pending {
                // Terminal announcements wait for the modal, but the one-time
                // waiting_user receipt is delivered so the user knows why the
                // device stopped responding to spoken requests.
                crate::desktop_agent::take_permission_notification(&session.id,&board)
            } else {
                crate::desktop_agent::take_notification(&session.id,&board)
            };
            if let Some(message)=notification {
                diagnostics::record(&session.id,"desktop_receipt",json!({"stage":"playback_started", "chars":message.chars().count()}));
                desktop_conversation.push("desktop_receipt", &message);
                let notification_turn=format!("{}-desktop-{}",session.id,now_ms());
                let _=player.send(PlayerCmd::Begin(notification_turn));
                publish(&app,&session,|s|{s.state="speaking".into();s.reply=message.clone();});
                hud(&usb,&board,&session.id,"speaking",&name,"");
                let announcement=async {
                    speak_one_logged(&mut tts,&player,&message,&session.id).await?;
                    wait_playback(&player).await;Ok(())
                };
                match listen_with_verified_barge_in(announcement,&mut rx,&mut deferred_audio,&mut vad,duplex).await {
                    Ok(speech)=>{
                        let silent_stop = speech.as_ref().is_some_and(|s|s.no_transcript_stop);
                        diagnostics::record(&session.id,"desktop_receipt",json!({"stage":"playback_finished", "ok":speech.as_ref().is_none_or(|s|s.reply_completed), "reason":if silent_stop {"no_transcript_stop"} else if speech.as_ref().is_some_and(|s|!s.reply_completed) {"interrupted"} else {"completed"}}));
                        if speech.as_ref().is_some_and(|s|!s.reply_completed) {let _=player.send(PlayerCmd::Flush);tts.interrupt();}
                        if silent_stop {
                            diagnostics::record(&session.id,"voice_stop",json!({"turn":session.status.lock().map(|s|s.turns).unwrap_or(0),"chars":0,"reason":"no_transcript"}));
                            vad.reset();
                            pending_speech=None;
                        } else {
                            pending_speech=speech;
                        }
                    }
                    Err(error)=>{diagnostics::record(&session.id,"desktop_receipt",json!({"stage":"playback_finished", "ok":false, "errorKind":diagnostics::error_kind(&error)}));let _=player.send(PlayerCmd::Flush);publish(&app,&session,|s|s.error=error);}
                }
                last_activity=Instant::now();desktop_progress.clear();
                publish(&app,&session,|s|s.state="listening".into());
                hud(&usb,&board,&session.id,"listening",&name,"");
                continue;
            }
            let progress = if approval_pending {
                "等待设备授权".to_string()
            } else {
                crate::desktop_agent::progress(&session.id,&board).unwrap_or_default()
            };
            if progress!=desktop_progress {
                hud(&usb,&board,&session.id,"listening",&name,&progress);
                desktop_progress=progress;
            }
        }
        if last_activity.elapsed() >= IDLE_TIMEOUT {
            reason = "idle".into();
            break;
        }
        if let Some(events) = asr_rx.as_mut() {
            while let Ok(event) = events.try_recv() {
                match event {
                    StreamingSpeechEvent::Partial { text, .. } => { if !text.trim().is_empty() { latest_asr = text; } }
                    StreamingSpeechEvent::Final { text, .. } => {
                        if !text.trim().is_empty() { latest_asr = text; }
                        final_asr = Some(latest_asr.clone());
                        recognizer_terminal = true;
                    }
                    StreamingSpeechEvent::Error(error) => {
                        asr_error=Some((error,"stream"));
                        continue 'conversation;
                    }
                    StreamingSpeechEvent::Ready => {
                        diagnostics::record(&session.id,"asr_ready",json!({"turn":turns+1,"elapsedMs":asr_started_at.elapsed().as_millis()}));
                    }
                }
            }
            if latest_asr != displayed_asr && caption_sent_at.elapsed() >= Duration::from_millis(200) {
                user_caption(&usb, &board, &session.id, &name, &latest_asr, false);
                publish(&app, &session, |s| s.transcript = latest_asr.clone());
                displayed_asr = latest_asr.clone();
                caption_sent_at = Instant::now();
            }
        }
        // A terminal ASR packet closes its worker. Finalize immediately rather
        // than writing more PCM into a closed recognizer or waiting for USB input.
        // This sentinel is control flow only: it is never sent to ASR or logged as audio.
        let next = if recognizer_terminal && recognizer.is_some() {
            Input::Pcm(vec![0; FRAME_BYTES], Some(false))
        } else if let Some(item) = deferred_audio.pop_front() { item } else { match timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(item)) => item,
            Ok(None) => { reason = "closed".into(); break; }
            Err(_) => continue,
        }};
        match next {
            Input::Pcm(pcm, speech) => {
                for frame in pcm.chunks(FRAME_BYTES) {
                    let step = if recognizer_terminal { vad.reset(); VadStep::End }
                        else { vad.step_in_context(frame, speech, VadContext::Listening) };
                    match step {
                        VadStep::Idle => {}
                        VadStep::Start(preroll) => {
                            asr_started_at = Instant::now();
                            latest_asr.clear();
                            displayed_asr.clear();
                            final_asr = None;
                            recognizer_terminal = false;
                            user_caption(&usb, &board, &session.id, &name, "", false);
                            last_activity = Instant::now();
                            let (tx, rx_events) = mpsc::unbounded_channel();
                            match StreamingSpeechRecognizer::start(move |event| { let _ = tx.send(event); }) {
                                Ok(rec) => {
                                    capture_stats = AsrCaptureStats::default();
                                    for f in preroll {
                                        if let Err(error) = rec.push_pcm(&f) {
                                            asr_error=Some((error,"preroll")); continue 'conversation;
                                        }
                                        capture_stats.observe(&f);
                                    }
                                    // The trigger frame is already part of preroll.
                                    recognizer = Some(rec);
                                    asr_rx = Some(rx_events);
                                    publish(&app, &session, |s| { s.state = "listening".into(); s.transcript.clear(); s.error.clear(); s.reason.clear(); });
                                    diagnostics::record(&session.id, "asr_started", json!({"turn": turns + 1}));
                                }
                                Err(error) => {
                                    asr_error=Some((error,"start")); continue 'conversation;
                                }
                            }
                        }
                        VadStep::Speech => {
                            if let Some(rec) = recognizer.as_ref() {
                                if let Err(error) = rec.push_pcm(frame) {
                                    asr_error=Some((error,"audio_push")); continue 'conversation;
                                }
                                capture_stats.observe(frame);
                            }
                        }
                        VadStep::End => {
                            let Some(rec) = recognizer.take() else { continue };
                            let Some(mut events) = asr_rx.take() else { continue };
                            let asr_finish_at = Instant::now();
                            if !recognizer_terminal {
                                if let Err(error) = rec.push_pcm(frame).and_then(|_| rec.finish()) {
                                    asr_error=Some((error,"finish")); continue 'conversation;
                                }
                                capture_stats.observe(frame);
                            }
                            recognizer_terminal = false;
                            capture_stats.record(&session.id, turns+1);
                            publish(&app, &session, |s| s.state = "thinking".into());
                            hud(&usb, &board, &session.id, "thinking", &name, "正在处理");
                            let final_result = match final_asr.take() {
                                Some(text) => Ok(text),
                                None => await_with_capture(await_final_text(&mut events, latest_asr.clone()), &mut rx, &mut deferred_audio, duplex).await,
                            };
                            let text = match final_result {
                                Ok(text) => text,
                                Err(error) => {
                                    diagnostics::record(&session.id,"asr_final_wait_failed",json!({"turn":turns+1,"elapsedMs":asr_finish_at.elapsed().as_millis(),"errorKind":diagnostics::error_kind(&error),"ok":false}));
                                    asr_error=Some((error,"final_wait")); continue 'conversation;
                                }
                            };
                            drop(rec);
                            asr_timeouts = 0;
                            let text = text.trim().to_string();
                            if text.is_empty() {
                                diagnostics::record(&session.id, "asr_empty", json!({"turn":turns+1,"elapsedMs":asr_finish_at.elapsed().as_millis(),"reason":"no_transcript"}));
                                publish(&app, &session, |s| s.state = "listening".into());
                                // VAD can be triggered by environmental noise. No transcript is
                                // not proof the user spoke; keep listening without blaming them.
                                hud(&usb, &board, &session.id, "listening", &name, "");
                                continue;
                            }
                            if is_explicit_stop(&text) {
                                // Stop means stop speaking, not a new LLM request
                                // that starts another spoken acknowledgement.
                                diagnostics::record(&session.id,"voice_stop",json!({"turn":turns,"chars":1}));
                                vad.reset();
                                publish(&app, &session, |s| {s.state="listening".into(); s.transcript=text.clone();});
                                hud(&usb, &board, &session.id, "listening", &name, "");
                                continue;
                            }
                            if crate::dsh_approval::device_approval_pending() {
                                // A device approval is modal and must be settled by
                                // keys/native UI; a spoken "允许" is chat text and must
                                // not reach the model or authorize anything.
                                diagnostics::record(&session.id,"approval_gated",json!({
                                    "turn":turns+1,
                                    "chars":text.chars().count(),
                                    "ok":false,
                                }));
                                publish(&app,&session,|s| {s.state="listening".into(); s.transcript=text.clone();});
                                hud(&usb,&board,&session.id,"listening",&name,"等待设备授权");
                                vad.reset();
                                last_activity=Instant::now();
                                continue;
                            }
                            turns += 1;
                            if let Err(error) = apply_pending_persona(&session, &mut input, &mut tts).await {
                                finish(&app, &usb, &session, "config", &error).await; return;
                            }
                            name = input.display_name.clone();
                            publish(&app, &session, |s| s.display_name = name.clone());
                            let user_text = text.clone();
                            user_caption(&usb, &board, &session.id, &name, &user_text, true);
                            hud(&usb, &board, &session.id, "thinking", &name, "");
                            publish(&app, &session, |s| { s.transcript = user_text.clone(); s.turns = turns; });
                            diagnostics::record(&session.id, "asr_final", json!({"turn":turns,"chars":user_text.chars().count(),"elapsedMs":asr_finish_at.elapsed().as_millis()}));
                            vad.reset();
                            vad.previous_user = user_text.clone();
                            vad.turn_started = Some(Instant::now());
                            match listen_with_verified_barge_in(answer_turn(&app, &usb, &session, &llm_cfg, &input.system_prompt, &mut history, &mut desktop_conversation, &user_text, &mut tts, &player, &name), &mut rx, &mut deferred_audio, &mut vad, duplex).await {
                                Ok(Some(speech)) => {
                                    if !speech.reply_completed {
                                        // The reply future is now dropped: aborts LLM and TTS callbacks.
                                        publish(&app, &session, clear_search);
                                        let _ = player.send(PlayerCmd::Flush);
                                        tts.interrupt();
                                        crate::smart_home::cancel_voice_session(&session.id);
                                        let reason = if speech.no_transcript_stop { "no_transcript_short_command" } else { "asr_confirmed" };
                                        diagnostics::record(&session.id,"barge_in",json!({"turn":turns,"bytes":speech.stats.frames*FRAME_BYTES,"reason":reason}));
                                        let partial=session.status.lock().map(|s|s.reply.clone()).unwrap_or_default();
                                        remember_interrupted_reply(&mut history,&mut desktop_conversation,&user_text,&partial);
                                    }
                                    if speech.no_transcript_stop {
                                        // The cloud lost the words, so this is a stop,
                                        // not a new user turn. Keep listening.
                                        diagnostics::record(&session.id,"voice_stop",json!({"turn":turns,"chars":0,"reason":"no_transcript"}));
                                        vad.reset();
                                        publish(&app, &session, |s| {s.state="listening".into(); s.transcript.clear();});
                                        hud(&usb, &board, &session.id, "listening", &name, "");
                                    } else {
                                        pending_speech = Some(speech);
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    diagnostics::record(&session.id, "turn_failed", json!({"errorKind":diagnostics::error_kind(&error)}));
                                    crate::smart_home::clear_voice_pending(&session.id).await;
                                    let _ = player.send(PlayerCmd::Flush);
                                    publish(&app, &session, |s| s.error = error.clone());
                                    hud(&usb, &board, &session.id, "listening", &name, "刚才没说成，请再说一遍");
                                }
                            }
                            if pending_speech.is_none() && crate::music_player::take_voice_handoff() {
                                reason = "music_playback".into();
                                break 'conversation;
                            }
                            if !duplex {
                                tokio::time::sleep(POST_SPEAK_GUARD).await;
                                drain_pending(&mut rx);
                                vad.reset();
                            }
                            last_activity = Instant::now();
                            publish(&app, &session, |s| { s.state = "listening".into(); if s.search_state=="searching" {clear_search(s);} });
                            hud(&usb, &board, &session.id, "listening", &name, "");
                        }
                    }
                }
            }
        }
    }
    if let Some(rec) = recognizer.take() { rec.cancel(); }
    let _ = player.send(PlayerCmd::Flush);
    finish(&app, &usb, &session, &reason, "").await;
}

/// Streaming ASR is speculative until it yields at least two meaningful characters
/// or the explicit single-character stop command “停”.
/// The same recognizer is then handed to the conversation loop, preserving the
/// first syllables without replaying PCM or opening a second cloud request.
struct VerifiedSpeech {
    recognizer: StreamingSpeechRecognizer,
    events: mpsc::UnboundedReceiver<StreamingSpeechEvent>,
    stats: AsrCaptureStats,
    text: String,
    reply_completed: bool,
    terminal: bool,
    /// The cloud Final carried no text; a short, clearly voiced near-end
    /// command is the only evidence. It stops playback but is not handed to
    /// the conversation loop as a new user turn.
    no_transcript_stop: bool,
}

fn is_explicit_stop(text: &str) -> bool {
    let mut chars = text.chars().filter(|ch| ch.is_alphanumeric());
    chars.next() == Some('停') && chars.next().is_none()
}

fn has_barge_in_text(text: &str) -> bool {
    let mut chars = text.chars().filter(|ch| ch.is_alphanumeric());
    match (chars.next(), chars.next()) {
        (Some('停'), None) | (Some(_), Some(_)) => true,
        _ => false,
    }
}

/// Why a barge probe should be finalized now, if at all. The silence window is
/// the short-command path; VAD End remains the bound for longer speech. The
/// frame cap is applied by the caller next to `BARGE_CONFIRM_MAX_FRAMES`.
/// It only opens after the same eight voiced frames the barge policy requires,
/// so noise cannot force an early Final.
fn barge_finish_reason(voiced_frames: usize, unvoiced_run: usize, vad_end: bool) -> Option<&'static str> {
    if vad_end { return Some("vad_end"); }
    if voiced_frames >= BARGE_FINISH_MIN_VOICED_FRAMES && unvoiced_run >= BARGE_FINISH_SILENCE_FRAMES {
        return Some("silence_window");
    }
    None
}

fn start_barge_probe(frames: Vec<Vec<u8>>) -> Result<VerifiedSpeech, String> {
    let (tx, events) = mpsc::unbounded_channel();
    let recognizer = StreamingSpeechRecognizer::start(move |event| { let _ = tx.send(event); })?;
    let mut stats = AsrCaptureStats::default();
    for frame in frames {
        recognizer.push_pcm(&frame)?;
        stats.observe(&frame);
    }
    Ok(VerifiedSpeech { recognizer, events, stats, text: String::new(), reply_completed: false, terminal: false, no_transcript_stop: false })
}

async fn listen_with_verified_barge_in<F>(reply: F, rx: &mut mpsc::Receiver<Input>, deferred: &mut VecDeque<Input>, vad: &mut Vad, duplex: bool)
    -> Result<Option<VerifiedSpeech>, String>
where F: std::future::Future<Output=Result<(), String>> {
    verify_barge_in_with(reply, rx, deferred, vad, duplex, start_barge_probe).await
}

async fn verify_barge_in_with<F, S>(reply: F, rx: &mut mpsc::Receiver<Input>, deferred: &mut VecDeque<Input>, vad: &mut Vad, duplex: bool, mut start: S)
    -> Result<Option<VerifiedSpeech>, String>
where F: std::future::Future<Output=Result<(), String>>,
      S: FnMut(Vec<Vec<u8>>) -> Result<VerifiedSpeech, String> {
    if !duplex { reply.await?; return Ok(None); }
    tokio::pin!(reply);
    loop {
        // Borrow, don't drop the reply future when VAD finds a candidate. Until
        // ASR confirms it, synthesis, queued PCM and the current sentence survive.
        let Some(frames) = listen_during_reply(reply.as_mut(), rx, deferred, vad, true).await? else { return Ok(None); };
        let candidate_phase=vad.session.as_ref().and_then(|s|s.status.lock().ok().map(|s|s.state.clone())).unwrap_or_else(||"thinking".into());
        // Capture provenance now: cloud ASR may return seconds later. Measuring
        // the tail window when the result arrives loses protection against the
        // just-finished utterance and can cancel its pending todo write.
        let previous_user = if barge_policy::protect_previous_tail(vad.turn_started.map(|t| t.elapsed())) {
            vad.previous_user.clone()
        } else { String::new() };
        diagnostics::record(&vad.diagnostic_session, "barge_candidate", json!({"frames":frames.len(),"bufferedMs":deferred.len()*20,"state":candidate_phase}));
        // Evidence for the no-transcript fallback starts from exactly the
        // frames that satisfied the barge gate (`vad.speech_run`), not from the
        // older preroll. Only periodic near-end voice counts; noise left after
        // AEC contributes nothing.
        let mut evidence = barge_policy::NoTranscriptEvidence::default();
        let seeded = vad.speech_run.min(frames.len());
        for frame in &frames[frames.len() - seeded..] {
            if periodic_voice_frame(frame) { evidence.observe_periodic(Vad::rms(frame)); }
        }
        let mut probe = match start(frames) {
            Ok(probe) => probe,
            Err(error) => {
                diagnostics::record(&vad.diagnostic_session,"barge_rejected",json!({"reason":"asr_start_failed","errorKind":diagnostics::error_kind(&error)}));
                vad.reset();
                continue;
            }
        };
        let started = Instant::now();
        let deadline = tokio::time::sleep(BARGE_CONFIRM_TIMEOUT);
        tokio::pin!(deadline);
        let mut added_frames = 0;
        let mut voiced_frames = vad.speech_run;
        let mut unvoiced_run = 0usize;
        let mut confirmation = barge_policy::Confirmation::default();
        let mut input_finished = false;
        // Any textual Partial proves the cloud heard something. The fallback
        // then stays closed even if the Final itself carries no words.
        let mut heard_partial_text = false;
        let mut rejection = "no_transcript";
        loop {
            // Poll ASR and the reply even when Windows has delivered a burst of
            // retained frames. A backlog must not starve confirmation or playback.
            tokio::select! {
                biased;
                _ = &mut deadline => {
                    // A short "等等" can lose its Final entirely. If the ASR
                    // deadline expires with no transcript, use the same bounded
                    // acoustic fallback rather than discarding the barge.
                    let phase = vad.session.as_ref().and_then(|s| s.status.lock().ok().map(|s| s.state.clone()))
                        .unwrap_or_else(|| "thinking".into());
                    if barge_policy::no_transcript_short_command(&phase, probe.reply_completed, input_finished, heard_partial_text, "", voiced_frames, evidence) {
                        probe.text.clear();
                        probe.terminal = true;
                        probe.no_transcript_stop = true;
                        diagnostics::record(&vad.diagnostic_session,"barge_confirmed",json!({
                            "chars":0,"elapsedMs":started.elapsed().as_millis(),"frames":probe.stats.frames,
                            "state":phase,"reason":"timeout_no_transcript_short_command",
                            "voicedFrames":voiced_frames,"periodicFrames":evidence.periodic_frames,
                            "peakRms":evidence.peak_rms.round()}));
                        return Ok(Some(probe));
                    }
                    rejection = "confirmation_timeout"; break;
                }
                event = probe.events.recv() => match event {
                    Some(event @ (StreamingSpeechEvent::Partial { .. } | StreamingSpeechEvent::Final { .. })) => {
                        let (text, terminal, confidence) = match event {
                            StreamingSpeechEvent::Partial {text,confidence,..} => (text,false,confidence),
                            StreamingSpeechEvent::Final {text,confidence,..} => (text,true,confidence),
                            _ => unreachable!(),
                        };
                        if !terminal && !barge_policy::normalized(&text).is_empty() {
                            heard_partial_text = true;
                        }
                        // Terminal empty/single-character candidates cannot improve.
                        // Reject now instead of leaving a completed stream polling.
                        // The one exception is a Final that carried no text at all
                        // after `finish()` was requested: for the diagnosed short
                        // "停"/"等等" failure the cloud loses the words entirely.
                        // That fallback still requires short periodic near-end voice
                        // and an actively playing reply; see the barge policy.
                        if terminal && !has_barge_in_text(&text) {
                            let phase = vad.session.as_ref().and_then(|s| s.status.lock().ok().map(|s| s.state.clone()))
                                .unwrap_or_else(|| "thinking".into());
                            if barge_policy::no_transcript_short_command(&phase, probe.reply_completed, input_finished, heard_partial_text, &text, voiced_frames, evidence) {
                                probe.text.clear();
                                probe.terminal = true;
                                probe.no_transcript_stop = true;
                                diagnostics::record(&vad.diagnostic_session,"barge_confirmed",json!({
                                    "chars":0,"elapsedMs":started.elapsed().as_millis(),"frames":probe.stats.frames,
                                    "state":phase,"reason":"no_transcript_short_command",
                                    "voicedFrames":voiced_frames,"periodicFrames":evidence.periodic_frames,
                                    "peakRms":evidence.peak_rms.round()}));
                                return Ok(Some(probe));
                            }
                            break;
                        }
                        if !has_barge_in_text(&text) { continue; }
                        if input_finished && !terminal { continue; }
                        let (phase, reply_text) = vad.session.as_ref().and_then(|s| s.status.lock().ok().map(|s| (s.state.clone(),s.reply.clone())))
                            .unwrap_or_else(|| ("thinking".into(),String::new()));
                        if !confirmation.accept(&text, terminal, confidence, started.elapsed(), voiced_frames, &phase, &reply_text, &previous_user) {
                            if terminal { rejection="unverified_speech"; break; }
                            continue;
                        }
                        probe.text = text;
                        probe.terminal = terminal;
                        // The candidate may have ended while waiting for cloud ASR.
                        // Continue with the SAME stream; ordinary silence detection
                        // sends its final marker once, without losing later speech.
                        vad.in_speech = true;
                        vad.silence_run = 0;
                        vad.speech_started.get_or_insert_with(Instant::now);
                        diagnostics::record(&vad.diagnostic_session,"barge_confirmed",json!({"chars":probe.text.chars().filter(|ch|ch.is_alphanumeric()).count(),"elapsedMs":started.elapsed().as_millis(),"frames":probe.stats.frames,"state":phase,"reason":if terminal {"final_transcript"} else if is_explicit_stop(&probe.text) {"explicit_stop"} else {"stable_transcript"}}));
                        return Ok(Some(probe));
                    }
                    Some(StreamingSpeechEvent::Error(error)) => {
                        diagnostics::record(&vad.diagnostic_session,"barge_probe_error",json!({"errorKind":diagnostics::error_kind(&error)}));
                        rejection = "asr_error"; break;
                    }
                    Some(StreamingSpeechEvent::Ready) => {},
                    None => break,
                },
                result = &mut reply, if !probe.reply_completed => {
                    result?;
                    probe.reply_completed = true;
                    vad.reply_tail_until = Some(Instant::now() + POST_SPEAK_GUARD);
                    // A barge candidate that is still active when the reply ends
                    // must still get a Final request. Only finalize here when the
                    // bounded policy already allows it; otherwise keep polling
                    // until the silence window, so a late confirmed Partial is
                    // not discarded just because playback finished first.
                    if !input_finished {
                        if let Some(reason) = barge_finish_reason(voiced_frames, unvoiced_run, false) {
                            if probe.recognizer.finish().is_err() { rejection="asr_error"; break; }
                            input_finished = true;
                            diagnostics::record(&vad.diagnostic_session,"barge_probe_finish",json!({"reason":reason,"frames":probe.stats.frames,"elapsedMs":started.elapsed().as_millis()}));
                        }
                    }
                }
                incoming = async { if !input_finished && !deferred.is_empty() { deferred.pop_front() } else { rx.recv().await } } => {
                    let Some(Input::Pcm(pcm, speech)) = incoming else { return Err("设备收音通道关闭".into()); };
                    if speech.is_none() || pcm.len() != FRAME_BYTES { return Err("设备 AEC/VAD 音频帧无效".into()); }
                    if input_finished {
                        if deferred.len() >= FINAL_CAPTURE_LIMIT { return Err("收音缓冲已满".into()); }
                        deferred.push_back(Input::Pcm(pcm,speech));
                        continue;
                    }
                    if let Err(error) = probe.recognizer.push_pcm(&pcm) {
                        diagnostics::record(&vad.diagnostic_session,"barge_probe_error",json!({"errorKind":diagnostics::error_kind(&error)}));
                        rejection = "asr_error"; break;
                    }
                    probe.stats.observe(&pcm);
                    added_frames += 1;
                    let frame_rms = Vad::rms(&pcm);
                    if speech == Some(true) && frame_rms >= PROCESSED_VAD_ABS_MIN {
                        voiced_frames += 1; unvoiced_run = 0;
                        if periodic_voice_frame(&pcm) { evidence.observe_periodic(frame_rms); }
                    } else {
                        unvoiced_run += 1;
                    }
                    // Keep feeding silence to streaming ASR; an empty VAD end is
                    // never itself permission to cancel the reply. The explicit
                    // finish policy is the only place that asks for the Final.
                    let vad_end = matches!(vad.step_with_hint(&pcm, speech), VadStep::End);
                    let finish_reason = barge_finish_reason(voiced_frames, unvoiced_run, vad_end)
                        .or_else(|| (added_frames >= BARGE_CONFIRM_MAX_FRAMES).then_some("frame_cap"));
                    if let Some(reason) = finish_reason {
                        if probe.recognizer.finish().is_err() { rejection="asr_error"; break; }
                        input_finished = true;
                        diagnostics::record(&vad.diagnostic_session,"barge_probe_finish",json!({"reason":reason,"frames":probe.stats.frames,"elapsedMs":started.elapsed().as_millis()}));
                    }
                }
            }
        }
        diagnostics::record(&vad.diagnostic_session,"barge_rejected",json!({"reason":rejection,"elapsedMs":started.elapsed().as_millis(),"frames":probe.stats.frames,"voicedFrames":voiced_frames,"periodicFrames":evidence.periodic_frames,"peakRms":evidence.peak_rms.round()}));
        probe.recognizer.cancel();
        vad.reset();
        if probe.reply_completed { return Ok(None); }
    }
}

/// Capture remains polled while the reply is thinking, synthesizing or playing.
/// Only AEC/VAD-tagged device PCM enables this branch. The verification wrapper
/// borrows `reply` here, so finding a candidate does not cancel playback.
async fn listen_during_reply<F>(reply: F, rx: &mut mpsc::Receiver<Input>, deferred: &mut VecDeque<Input>, vad: &mut Vad, duplex: bool)
    -> Result<Option<Vec<Vec<u8>>>, String>
where F: std::future::Future<Output=Result<(), String>> {
    if !duplex {reply.await?; return Ok(None);}
    tokio::pin!(reply);
    loop {
        // Process audio retained during the ASR final wait before starting a
        // reply. Any resumed speech can cancel the reply without losing its tail.
        let incoming = if let Some(input) = deferred.pop_front() { Some(input) } else { tokio::select! {
            result = &mut reply => {
                // Continue listening, but retain playback-level echo rejection
                // for the brief acoustic/DMA tail. Never discard these frames.
                vad.reply_tail_until = Some(Instant::now() + POST_SPEAK_GUARD);
                result?; return Ok(None);
            }
            incoming = rx.recv() => incoming,
        }};
        let Some(Input::Pcm(pcm, speech)) = incoming else {return Err("设备收音通道关闭".into());};
        if speech.is_none() {return Err("设备 AEC/VAD 不可用，请重新进入对话".into());}
        for frame in pcm.chunks(FRAME_BYTES) {
            if let VadStep::Start(preroll) = vad.step_with_hint(frame, speech) {return Ok(Some(preroll));}
        }
    }
}

/// ASR can take up to eight seconds to finalize; the live ingress queue only
/// holds 2.56 seconds. Keep draining it without discarding a user's next words.
/// The bounded backlog is replayed through the same VAD before any new reply.
async fn await_with_capture<F, T>(result: F, rx: &mut mpsc::Receiver<Input>, deferred: &mut VecDeque<Input>, duplex: bool) -> Result<T, String>
where F: std::future::Future<Output=Result<T, String>> {
    if !duplex { return result.await; }
    tokio::pin!(result);
    loop {
        tokio::select! {
            value = &mut result => return value,
            incoming = rx.recv() => {
                let Some(Input::Pcm(pcm, speech)) = incoming else {return Err("设备收音通道关闭".into());};
                if speech.is_none() || pcm.len() != FRAME_BYTES { return Err("设备 AEC/VAD 音频帧无效".into()); }
                if deferred.len() >= FINAL_CAPTURE_LIMIT { return Err("语音识别等待过久，收音缓冲已满，请重新开始对话".into()); }
                deferred.push_back(Input::Pcm(pcm, speech));
            }
        }
    }
}

fn drain_pending(rx: &mut mpsc::Receiver<Input>) {
    // Control cancellation has its own Notify and cannot be dropped with old microphone frames.
    while rx.try_recv().is_ok() {}
}

fn queue_persona_update(session: &Session, input: RealtimeChatStartInput) -> Result<bool, String> {
    let status = session.status.lock().map_err(|_| "实时对话状态锁异常")?;
    if !status.active || session.cancelled.load(Ordering::SeqCst) || status.appearance_id != input.appearance_id { return Ok(false); }
    if input.system_prompt.trim().is_empty() || input.display_name.trim().is_empty() { return Err("人设名称或内容不能为空".into()); }
    let mut pending = session.pending_persona.lock().map_err(|_| "实时对话配置锁异常")?;
    *pending = Some(input);
    Ok(true)
}

async fn apply_pending_persona(session: &Session, current: &mut RealtimeChatStartInput, tts: &mut DoubaoTtsClient) -> Result<bool, String> {
    apply_pending_persona_with(session, current, tts, |voice| {
        voice_chat_settings::tts_config(&voice.speaker, &voice.clone_speaker_id, voice.speed())
    }).await
}

async fn apply_pending_persona_with(session: &Session, current: &mut RealtimeChatStartInput, tts: &mut DoubaoTtsClient,
    config_for: impl FnOnce(&VoiceSpec) -> Result<crate::doubao_tts::DoubaoTtsConfig, String>) -> Result<bool, String> {
    // Separate from the PCM queue: playback cleanup must never discard a saved update.
    let pending = session.pending_persona.lock().map_err(|_| "实时对话配置锁异常")?.take();
    let Some(next) = pending else { return Ok(false) };
    if next.voice.speaker != current.voice.speaker || next.voice.clone_speaker_id != current.voice.clone_speaker_id
        || next.voice.speed() != current.voice.speed() {
        let cfg = config_for(&next.voice)?;
        tts.reset().await;
        *tts = DoubaoTtsClient::new(cfg);
    }
    current.display_name = next.display_name;
    current.system_prompt = next.system_prompt;
    current.greeting = next.greeting;
    current.voice = next.voice;
    Ok(true)
}

#[tauri::command]
pub fn realtime_chat_update_persona(input: RealtimeChatStartInput) -> Result<bool, String> {
    match active_session() { Some(session) => queue_persona_update(&session, input), None => Ok(false) }
}

#[tauri::command]
pub fn load_realtime_voice_volume() -> Result<f32, String> {
    voice_chat_settings::realtime_volume()
}

#[tauri::command]
pub fn save_realtime_voice_volume(volume: f32) -> Result<f32, String> {
    // Share the start barrier so a concurrent start cannot publish an old gain
    // after a successful save. Persist first; errors leave live audio unchanged.
    let _guard = start_guard().lock().map_err(|_| "实时对话启动锁异常")?;
    let saved = voice_chat_settings::save_realtime_volume(volume)?;
    if let Some(session) = active_session() {
        session.set_playback_volume(&VoiceSpec { volume:saved, ..Default::default() });
        diagnostics::record(&session.id, "voice_volume_updated", json!({"volume":saved}));
    }
    Ok(saved)
}

fn recoverable_asr_error(error: &str) -> bool {
    // Authentication/configuration errors still need user action; do not loop
    // blindly or retry paid requests. Recovery only permits a NEW utterance.
    diagnostics::error_kind(error) == "timeout"
}

fn reset_timed_out_asr(vad: &mut Vad, latest: &mut String, displayed: &mut String,
    final_text: &mut Option<String>, terminal: &mut bool, deferred: &mut VecDeque<Input>) {
    vad.reset();
    latest.clear(); displayed.clear(); *final_text=None; *terminal=false;
    // Never re-feed stale audio into a new paid recognition attempt. Stop and
    // persona updates live outside this PCM queue and are left untouched.
    deferred.retain(|input| !matches!(input, Input::Pcm(..)));
}

async fn await_final_text(events: &mut mpsc::UnboundedReceiver<StreamingSpeechEvent>, latest: String) -> Result<String, String> {
    await_final_text_with_timeout(events, latest, ASR_FINAL_TIMEOUT).await
}

async fn await_final_text_with_timeout(events: &mut mpsc::UnboundedReceiver<StreamingSpeechEvent>, mut latest: String, budget: Duration) -> Result<String, String> {
    let deadline = Instant::now() + budget;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("语音识别结果超时".into());
        }
        match timeout(remaining, events.recv()).await {
            Ok(Some(StreamingSpeechEvent::Final { text, .. })) => return Ok(if text.trim().is_empty() { latest } else { text }),
            Ok(Some(StreamingSpeechEvent::Partial { text, .. })) => { if !text.trim().is_empty() { latest = text; } }
            Ok(Some(StreamingSpeechEvent::Error(error))) => {
                return Err(error);
            }
            Ok(Some(StreamingSpeechEvent::Ready)) => {}
            Err(_) => return Err("语音识别结果超时".into()),
            Ok(None) => return if latest.is_empty() { Err("语音识别没有返回结果，请检查服务连接".into()) } else { Ok(latest) },
        }
    }
}

fn remember_interrupted_reply(history:&mut Vec<ChatTurn>, conversation:&mut crate::desktop_agent::Conversation,
    user:&str, displayed:&str) {
    history.push(ChatTurn{role:"user".into(),content:user.into()});
    if !displayed.trim().is_empty() && !persona_llm::is_internal_reply(displayed) {
        let partial=format!("中断前显示的未完整播报内容（仅作上下文，不能据此推断执行结果）：{displayed}");
        history.push(ChatTurn{role:"assistant".into(),content:partial.clone()});
        conversation.push("pet_partial",&partial);
    }
    history.push(persona_llm::interruption_turn());
    conversation.push("interrupted","本轮回复被打断；已发出的操作不会撤回，执行结果须核对，不得自动重复。");
    persona_llm::trim_history(history);
}

#[allow(clippy::too_many_arguments)]
async fn answer_turn(
    app: &AppHandle,
    usb: &UsbSerialManager,
    session: &Arc<Session>,
    llm_cfg: &persona_llm::LlmConfig,
    system_prompt: &str,
    history: &mut Vec<ChatTurn>,
    desktop_conversation: &mut crate::desktop_agent::Conversation,
    user_text: &str,
    tts: &mut DoubaoTtsClient,
    player: &Player,
    name: &str,
) -> Result<(), String> {
    let board = session.board_device_id.clone();
    enum ReplyEvent { Sentence(String), Search(crate::web_search::Event) }
    // This receiver belongs only to this reply. Dropping it on a verified
    // interruption makes late events unreachable by subsequent turns.
    let (sentence_tx, mut sentence_rx) = mpsc::unbounded_channel::<ReplyEvent>();
    let search_tx=sentence_tx.clone();
    publish(app,session,|s| {clear_search(s); s.reply.clear(); s.error.clear();});
    let mut splitter = SentenceSplitter::default();
    let llm_cfg = llm_cfg.clone();
    let prompt = system_prompt.to_string();
    let history_snapshot = history.clone();
    let user = user_text.to_string();
    let turn_started = Instant::now();
    let log_session = session.id.clone();
    let desktop_context=crate::desktop_agent::Context::new(&session.id,&board,
        session.status.lock().map(|s|s.turns).unwrap_or(0),user_text)
        .with_conversation(desktop_conversation)
        .with_realtime_target(&session.target_agent_id,&session.target_session_id)
        .with_target_unconfirmed(session.target_unconfirmed);
    // Retain the user turn even if the reply future is interrupted or fails.
    desktop_conversation.push("user", user_text);
    diagnostics::record(&session.id, "llm_started", json!({}));
    let llm_task = tokio::spawn(async move {
        let mut first_delta = true;
        let mut first_sentence = true;
        let result = persona_llm::stream_chat_with_desktop(&llm_cfg, &prompt, &history_snapshot, &user, Some(&log_session), Some(desktop_context), |delta| {
            if first_delta {
                diagnostics::record(&log_session, "llm_first_delta", json!({"elapsedMs":turn_started.elapsed().as_millis()}));
                first_delta = false;
            }
            for sentence in splitter.push(delta) {
                if first_sentence {
                    diagnostics::record(&log_session, "llm_first_sentence", json!({"elapsedMs":turn_started.elapsed().as_millis()}));
                    first_sentence = false;
                }
                let _ = sentence_tx.send(ReplyEvent::Sentence(sentence));
            }
        }, |event| {let _ = search_tx.send(ReplyEvent::Search(event));})
        .await;
        if let Some(rest) = splitter.flush() {
            if first_sentence {
                diagnostics::record(&log_session, "llm_first_sentence", json!({"elapsedMs":turn_started.elapsed().as_millis()}));
            }
            let _ = sentence_tx.send(ReplyEvent::Sentence(rest));
        }
        result
    });
    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop { fn drop(&mut self) { self.0.abort(); } }
    let _abort = AbortOnDrop(llm_task.abort_handle());

    let turn_id = format!("{}-{}", session.id, now_ms());
    let mut spoken = false;
    let mut reply = String::new();
    let slow_hint = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(slow_hint);
    let mut hinted = false;
    loop {
        let event = tokio::select! {
            event = sentence_rx.recv() => match event { Some(event) => event, None => break },
            _ = &mut slow_hint, if !spoken && !hinted => {
                hinted = true;
                hud(usb,&board,&session.id,"thinking",name,"已听清，正在处理");
                diagnostics::record(&session.id,"model_wait_hint",json!({"elapsedMs":turn_started.elapsed().as_millis()}));
                continue;
            }
        };
        if session.cancelled.load(Ordering::SeqCst) {
            break;
        }
        let sentence=match event {
            ReplyEvent::Sentence(sentence)=>sentence,
            ReplyEvent::Search(event)=>{
                if matches!(event,crate::web_search::Event::Started) {
                    hinted = true;
                    hud(usb,&board,&session.id,"thinking",name,"正在联网查询");
                }
                publish(app,session,|s|update_search(s,event));
                continue;
            },
        };
        let clean = persona_llm::clean_spoken_text(&sentence);
        if persona_llm::is_internal_reply(&sentence) {
            diagnostics::record(&session.id,"internal_reply_blocked",json!({"turn":session.status.lock().map(|s|s.turns).unwrap_or(0)}));
            return Err("本轮回答异常，已拦截内部状态文案；请重新提问".into());
        }
        if clean.is_empty() {
            continue;
        }
        if !spoken {
            spoken = true;
            let _ = player.send(PlayerCmd::Begin(turn_id.clone()));
            publish(app, session, |s| s.state = "speaking".into());
            hud(usb, &board, &session.id, "speaking", name, "");
        }
        reply.push_str(&clean);
        let shown = reply.clone();
        publish(app, session, |s| s.reply = shown.clone());
        speak_one_logged(tts, player, &clean, &session.id).await?;
    }
    let full = match llm_task.await {
        Ok(Ok(text)) => text,
        Ok(Err(error)) => {
            if spoken {
                wait_playback(player).await;
            }
            return Err(error);
        }
        Err(error) => return Err(format!("对话任务异常: {error}")),
    };
    if spoken {
        wait_playback(player).await;
    } else {
        let fallback = "嗯……我刚才走神了，你再说一遍？";
        let _ = player.send(PlayerCmd::Begin(turn_id.clone()));
        speak_one(tts, player, fallback).await?;
        wait_playback(player).await;
    }
    crate::smart_home::arm_voice_pending(&session.id).await;
    diagnostics::record(&session.id, "turn_completed", json!({"chars": full.chars().count(),"elapsedMs":turn_started.elapsed().as_millis()}));
    history.push(ChatTurn { role: "user".into(), content: user_text.to_string() });
    let final_reply = if full.trim().is_empty() { reply } else { full };
    desktop_conversation.push("pet", &final_reply);
    history.push(ChatTurn { role: "assistant".into(), content: final_reply });
    persona_llm::trim_history(history);
    Ok(())
}

async fn speak_one(tts: &mut DoubaoTtsClient, player: &Player, text: &str) -> Result<(), String> {
    speak_one_logged(tts, player, text, "").await
}

async fn speak_one_logged(tts: &mut DoubaoTtsClient, player: &Player, text: &str, session_id: &str) -> Result<(), String> {
    let sender = player.clone();
    // Bind every callback to the epoch at synthesis start. If a barge-in or a
    // newer turn flushes the player, late TTS chunks must not inherit the new
    // epoch and mix the old sentence into the next response.
    let epoch = player.epoch();
    let started = Instant::now();
    let mut first_pcm = true;
    tts.synthesize(text, |pcm| {
        if first_pcm && !pcm.is_empty() {
            let _ = sender.send_at(epoch, PlayerCmd::Subtitle(text.to_string()));
            diagnostics::record(session_id, "tts_first_pcm", json!({"elapsedMs":started.elapsed().as_millis()}));
            first_pcm = false;
        }
        let _ = sender.send_at(epoch, PlayerCmd::Pcm(pcm.to_vec()));
    })
    .await
    .map(|_| ())
    .map_err(|error| {
        diagnostics::record(session_id,"tts_turn_failed",json!({"errorKind":diagnostics::error_kind(&error)}));
        format!("语音合成失败：{error}")
    })
}

async fn wait_playback(player: &Player) {
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    if player.send(PlayerCmd::End(done_tx)).is_err() {
        return;
    }
    let _ = tauri::async_runtime::spawn_blocking(move || {
        let _ = done_rx.recv_timeout(Duration::from_secs(120));
    })
    .await;
}

async fn finish(app: &AppHandle, usb: &UsbSerialManager, session: &Arc<Session>, reason: &str, error: &str) {
    crate::smart_home::clear_voice_pending(&session.id).await;
    session.cancelled.store(true, Ordering::SeqCst);
    let board = session.board_device_id.clone();
    send_board(usb, &board, "audio/play_flush", json!({ "sessionId": session.id }));
    send_board(usb, &board, "audio/conversation", json!({ "enabled": false, "sessionId": session.id }));
    let name = session.status.lock().map(|s| s.display_name.clone()).unwrap_or_default();
    let hint = match reason {
        "tts" => "语音合成失败，请查看 PC 实时对话提示",
        "asr" => "语音识别失败，请查看 PC 实时对话提示",
        _ => "实时对话失败，请查看 PC 提示",
    };
    hud(usb, &board, &session.id, if error.is_empty() { "ended" } else { "error" }, &name, if error.is_empty() { "" } else { hint });
    diagnostics::record(&session.id, "ended", json!({"reason": reason, "ok":error.is_empty(),"errorKind": if error.is_empty() { "none" } else { diagnostics::error_kind(error) }}));
    // Keep the final snapshot so reconnecting/mounting UI can still retrieve the failure.
    let reason = reason.to_string();
    let error = error.to_string();
    publish(app, session, |s| {
        if s.reason == "asr_timeout" { s.error.clear(); }
        s.active = false;
        if s.search_state=="searching" {clear_search(s);}
        s.state = "ended".into();
        s.reason = reason.clone();
        if !error.is_empty() {
            s.error = error.clone();
        }
    });
}

// ---- preview -----------------------------------------------------------------------------------

pub async fn preview(input: PersonaVoicePreviewInput) -> Result<(), String> {
    let text = input.text.trim().to_string();
    if text.is_empty() {
        return Err("没有可试听的文本".to_string());
    }
    let cfg = voice_chat_settings::tts_config(&input.voice.speaker, &input.voice.clone_speaker_id, input.voice.speed())?;
    let mut client = DoubaoTtsClient::new(cfg);
    let mut pcm: Vec<u8> = Vec::new();
    client.synthesize(&text, |chunk| pcm.extend_from_slice(chunk)).await?;
    client.reset().await;
    scale_pcm16(&mut pcm, input.voice.volume());
    tauri::async_runtime::spawn_blocking(move || {
        crate::pc_playback::play_pcm16_mono_16k(&pcm, Duration::from_secs(PREVIEW_MAX_SECONDS))
    })
    .await
    .map_err(|error| format!("试听播放线程异常: {error}"))?
}

// ---- tauri commands --------------------------------------------------------------------------

#[derive(Clone)]
struct CachedDshFocus {
    instance_id: String,
    focus: Option<crate::dsh_task::FocusSnapshot>,
}

fn dsh_focus_cache() -> &'static Mutex<Option<CachedDshFocus>> {
    static CACHE: OnceLock<Mutex<Option<CachedDshFocus>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn dsh_focus_sample_override() -> &'static Mutex<Option<crate::dsh_task::FocusSample>> {
    static OVERRIDE: OnceLock<Mutex<Option<crate::dsh_task::FocusSample>>> = OnceLock::new();
    OVERRIDE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn set_focus_sample_override(sample: Option<crate::dsh_task::FocusSample>) {
    if let Ok(mut slot) = dsh_focus_sample_override().lock() {
        *slot = sample;
    }
}

async fn sample_dsh_focus() -> crate::dsh_task::FocusSample {
    #[cfg(test)]
    if let Ok(slot) = dsh_focus_sample_override().lock() {
        if let Some(sample) = slot.clone() {
            return sample;
        }
    }
    crate::dsh_task::focus().await
}

/// Whether a DSH UI focus selection is newer than the P4 voice target.
/// An explicit P4 choice wins ties and missing timestamps; an auto P4 target
/// has no user-selected session, so any confirmed focus wins.
fn focus_precedes_p4(
    p4_session_id: &str,
    p4_updated_at: u64,
    focus_updated_at: u64,
    focus_origin: &str,
) -> bool {
    let p4_explicit = !p4_session_id.trim().is_empty()
        && !p4_session_id.trim().eq_ignore_ascii_case("auto");
    if !p4_explicit {
        return true;
    }
    // A renderer-restored selection is not a fresh user action and must never
    // override an explicit P4 choice after a DSH restart.
    if focus_origin == "restore" {
        return false;
    }
    p4_updated_at > 0 && focus_updated_at > p4_updated_at
}

fn focus_status_label(status: crate::dsh_task::FocusSampleStatus) -> &'static str {
    match status {
        crate::dsh_task::FocusSampleStatus::Ok => "ok",
        crate::dsh_task::FocusSampleStatus::Unsupported => "unsupported",
        crate::dsh_task::FocusSampleStatus::Unavailable => "unavailable",
    }
}

fn follows_dsh_target(requested_agent: &str, fallback_agent: &str) -> bool {
    let requested = requested_agent.trim();
    requested == "deepseek-harness"
        || (requested.is_empty() && fallback_agent == "deepseek-harness")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FocusDecision {
    KeepP4,
    UseFocus {
        focus: crate::dsh_task::FocusSnapshot,
        cached: bool,
    },
    Unconfirmed,
}

/// Pure DSH focus vs P4 target decision. `cached` is only consulted when the
/// sampled result cannot provide an attached-client selection. Explicit P4
/// choices win ties and missing timestamps; an auto P4 target loses to any
/// confirmed DSH focus.
fn decide_focus_target(
    p4_session_id: &str,
    p4_updated_at: u64,
    status: crate::dsh_task::FocusSampleStatus,
    observer_attached: bool,
    observer_capable: bool,
    focus: Option<&crate::dsh_task::FocusSnapshot>,
    bridge_reachable: bool,
    cached: Option<&crate::dsh_task::FocusSnapshot>,
) -> FocusDecision {
    let p4_explicit = !p4_session_id.trim().is_empty()
        && !p4_session_id.trim().eq_ignore_ascii_case("auto");
    let usable_focus = focus.filter(|focus| !focus.session_id.trim().is_empty());
    let usable_cached = cached.filter(|focus| !focus.session_id.trim().is_empty());
    match status {
        crate::dsh_task::FocusSampleStatus::Ok => {
            if let Some(focus) = usable_focus {
                if focus_precedes_p4(
                    p4_session_id,
                    p4_updated_at,
                    focus.updated_at,
                    &focus.origin,
                ) {
                    FocusDecision::UseFocus { focus: focus.clone(), cached: false }
                } else {
                    FocusDecision::KeepP4
                }
            } else if observer_attached && observer_capable {
                // The focus observer is installed and has no ordinary main
                // session: explicit no-selection; keep the P4 target.
                FocusDecision::KeepP4
            } else if let Some(cached) = usable_cached {
                if focus_precedes_p4(
                    p4_session_id,
                    p4_updated_at,
                    cached.updated_at,
                    &cached.origin,
                ) {
                    FocusDecision::UseFocus { focus: cached.clone(), cached: true }
                } else {
                    FocusDecision::KeepP4
                }
            } else if !p4_explicit {
                FocusDecision::Unconfirmed
            } else {
                FocusDecision::KeepP4
            }
        }
        crate::dsh_task::FocusSampleStatus::Unsupported => FocusDecision::KeepP4,
        crate::dsh_task::FocusSampleStatus::Unavailable => {
            if let Some(cached) = usable_cached {
                if focus_precedes_p4(
                    p4_session_id,
                    p4_updated_at,
                    cached.updated_at,
                    &cached.origin,
                ) {
                    FocusDecision::UseFocus { focus: cached.clone(), cached: true }
                } else {
                    FocusDecision::KeepP4
                }
            } else if !p4_explicit && bridge_reachable {
                FocusDecision::Unconfirmed
            } else {
                FocusDecision::KeepP4
            }
        }
    }
}

/// Merge the DSH Web/Desktop main-session selection into the realtime start
/// target. Runs before the session is created; the chosen target is then
/// pinned by the t15 mechanism for the whole conversation. Never silently
/// falls back to the current session when a focus was known but unreadable.
async fn resolve_dsh_focus_target(input: &mut RealtimeChatStartInput) {
    let requested_agent = input.target_agent_id.trim().to_string();
    let fallback_agent = if requested_agent.is_empty() {
        crate::resolve_usb_inject_agent_id()
    } else {
        String::new()
    };
    if !follows_dsh_target(&requested_agent, &fallback_agent) {
        input.target_unconfirmed = false;
        return;
    }
    if requested_agent.is_empty() {
        input.target_agent_id = "deepseek-harness".to_string();
    }
    let sample = sample_dsh_focus().await;
    let p4_session = input.target_session_id.trim().to_string();
    let p4_updated_at = input.target_updated_at;
    let p4_explicit = !p4_session.is_empty() && !p4_session.eq_ignore_ascii_case("auto");

    // A different bridge instance invalidates the previous cache.
    if sample.status == crate::dsh_task::FocusSampleStatus::Ok {
        if let Ok(mut cache) = dsh_focus_cache().lock() {
            if cache.as_ref().is_some_and(|cached| {
                !sample.instance_id.is_empty()
                    && !cached.instance_id.is_empty()
                    && cached.instance_id != sample.instance_id
            }) {
                *cache = None;
            }
        }
    }
    // Only an observer that is installed and capable can explicitly clear the
    // last confirmed focus. An SSE-only/old client must not erase it.
    if sample.status == crate::dsh_task::FocusSampleStatus::Ok
        && ((sample.observer_attached && sample.observer_capable) || sample.focus.is_some())
    {
        if let Ok(mut cache) = dsh_focus_cache().lock() {
            *cache = Some(CachedDshFocus {
                instance_id: sample.instance_id.clone(),
                focus: sample.focus.clone().filter(|focus| !focus.session_id.trim().is_empty()),
            });
        }
    }
    if sample.status == crate::dsh_task::FocusSampleStatus::Unsupported {
        if let Ok(mut cache) = dsh_focus_cache().lock() {
            *cache = None;
        }
    }
    let cached = dsh_focus_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.clone())
        .and_then(|cache| cache.focus);
    let decision = decide_focus_target(
        &p4_session,
        p4_updated_at,
        sample.status,
        sample.observer_attached,
        sample.observer_capable,
        sample.focus.as_ref(),
        sample.bridge_reachable,
        cached.as_ref(),
    );
    let mut source = "p4";
    let mut cached_used = false;
    let mut unconfirmed = false;
    match decision {
        FocusDecision::KeepP4 => {}
        FocusDecision::UseFocus { focus, cached } => {
            input.target_agent_id = "deepseek-harness".to_string();
            input.target_session_id = focus.session_id;
            input.target_updated_at = focus.updated_at;
            source = if cached { "dsh-ui-cached" } else { "dsh-ui" };
            cached_used = cached;
        }
        FocusDecision::Unconfirmed => {
            unconfirmed = true;
        }
    }
    input.target_unconfirmed = unconfirmed;
    crate::realtime_chat_log::record("", "dsh_focus_target", json!({
        "status": focus_status_label(sample.status),
        "clientAttached": sample.client_attached,
        "observerAttached": sample.observer_attached,
        "observerCapable": sample.observer_capable,
        "bridgeReachable": sample.bridge_reachable,
        "p4Explicit": p4_explicit,
        "cachedUsed": cached_used,
        "chosen": source,
        "unconfirmed": unconfirmed,
        "ok": !unconfirmed,
    }));
}

#[tauri::command]
pub async fn realtime_chat_start(app: AppHandle, mut input: RealtimeChatStartInput) -> Result<RealtimeChatStatus, String> {
    resolve_dsh_focus_target(&mut input).await;
    start(&app, input)
}

#[tauri::command]
pub async fn realtime_chat_stop(reason: Option<String>) -> Result<RealtimeChatStatus, String> {
    Ok(stop(reason.as_deref().unwrap_or("ui")))
}

#[tauri::command]
pub async fn realtime_chat_status() -> Result<RealtimeChatStatus, String> {
    Ok(status())
}

#[tauri::command]
pub async fn persona_voice_preview(input: PersonaVoicePreviewInput) -> Result<(), String> {
    preview(input).await
}

#[tauri::command]
pub async fn load_voice_chat_settings() -> Result<voice_chat_settings::VoiceChatSettingsStatus, String> {
    voice_chat_settings::status()
}

#[tauri::command]
pub async fn list_voice_chat_models(input: voice_chat_settings::VoiceChatSettingsInput) -> Result<Vec<String>, String> {
    voice_chat_settings::list_models(input).await
}

#[tauri::command]
pub async fn save_voice_chat_settings(
    input: voice_chat_settings::VoiceChatSettingsInput,
) -> Result<voice_chat_settings::VoiceChatSettingsStatus, String> {
    voice_chat_settings::save(input)
}

#[tauri::command]
pub async fn clear_voice_chat_secret(which: String) -> Result<voice_chat_settings::VoiceChatSettingsStatus, String> {
    voice_chat_settings::clear_secret(&which)
}

#[cfg(test)]
mod tests {
    #[test]
    fn interruption_keeps_displayed_question_without_claiming_it_was_completed() {
        let mut history=Vec::new();
        let mut conversation=crate::desktop_agent::Conversation::default();
        super::remember_interrupted_reply(&mut history,&mut conversation,"安装Chrome","要交给桌面助手安装吗？");
        assert_eq!(history[0].content,"安装Chrome");
        assert!(history[1].content.contains("要交给桌面助手安装吗？"));
        assert!(history[1].content.contains("不能据此推断执行结果"));
        assert_eq!(history[2].role,crate::persona_llm::interruption_turn().role);
        let mut empty=Vec::new();
        super::remember_interrupted_reply(&mut empty,&mut conversation,"安装Chrome","");
        assert_eq!(empty.len(),2);
    }
    #[test]
    fn output_gain_is_global_and_applies_once_even_to_already_queued_pcm() {
        use super::*;
        let (session, _) = routing_session();
        let raw: Vec<u8> = [10000i16, -10000, 0].iter().flat_map(|s| s.to_le_bytes()).collect();
        session.set_playback_volume(&VoiceSpec { volume:0.3, ..Default::default() });
        let (quiet, gain) = playback_pcm(&session, &raw);
        assert_eq!(gain,0.3);
        assert_eq!(quiet, [3000i16,-3000,0].iter().flat_map(|s| s.to_le_bytes()).collect::<Vec<_>>());
        session.set_playback_volume(&VoiceSpec { volume:1.0, ..Default::default() });
        assert_eq!(playback_pcm(&session,&raw).0,raw);
        // An unrelated persona save must not restore its legacy per-pet gain.
        let update = RealtimeChatStartInput { board_device_id:"board".into(),appearance_id:"".into(),
            display_name:"pet".into(),system_prompt:"prompt".into(),greeting:"".into(),
            voice:VoiceSpec {volume:1.5,..Default::default()}, target_agent_id:String::new(), target_session_id:String::new(),
            target_updated_at:0, target_unconfirmed:false };
        queue_persona_update(&session,update).unwrap();
        assert_eq!(session.playback_volume(),1.0);
    }
    fn routing_session() -> (super::Session, tokio::sync::mpsc::Receiver<super::Input>) {
        use super::*;
        let (input, rx) = mpsc::channel(8);
        (Session { id:"rtc-new".into(), board_device_id:"board".into(),
            cancelled:AtomicBool::new(false), duplex:AtomicBool::new(true),
            capture_armed:AtomicBool::new(false), capture_ready:AtomicBool::new(false),
            stop_notify:tokio::sync::Notify::new(), input,
            status:Mutex::new(RealtimeChatStatus {active:true, ..Default::default()}),
            completed_playback:Mutex::new(String::new()), pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits()) }, rx)
    }

    #[test]
    fn capture_routing_ignores_preentry_ptt_other_boards_and_old_timelines() {
        use super::*;
        let (session, mut rx) = routing_session();
        assert!(route_session_audio(&session, "audio/chunk", &json!({"aec":false})));
        assert!(!session.cancelled.load(Ordering::SeqCst));
        session.capture_armed.store(true, Ordering::SeqCst);
        assert!(!route_session_audio(&session, "audio/status", &json!({
            "boardDeviceId":"other", "conversation":true, "audioAec":false})));
        for payload in [json!({"conversationSessionId":"", "aec":false}),
            json!({"conversationSessionId":"rtc-old", "aec":false}),
            json!({"conversationSessionId":"rtc-old", "aec":true, "vadSpeech":true, "data":BASE64.encode(vec![0; FRAME_BYTES])})] {
            assert!(route_session_audio(&session, "audio/chunk", &payload));
        }
        assert!(route_session_audio(&session, "audio/status", &json!({
            "conversationSessionId":"rtc-old", "conversation":true, "audioAec":false})));
        assert!(route_session_audio(&session, "audio/status", &json!({"conversation":false,"audioAec":false})));
        assert!(!session.cancelled.load(Ordering::SeqCst));
        assert!(!session.capture_ready.load(Ordering::SeqCst));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn aec_preparation_waits_then_routes_only_processed_current_audio() {
        use super::*;
        let (session, mut rx) = routing_session();
        session.capture_armed.store(true, Ordering::SeqCst);
        assert!(route_session_audio(&session, "audio/status", &json!({
            "conversationSessionId":"rtc-new", "conversation":true,
            "audioAec":false, "audioAecPreparing":true})));
        assert!(!session.cancelled.load(Ordering::SeqCst));
        assert!(!session.capture_ready.load(Ordering::SeqCst));
        assert!(route_session_audio(&session, "audio/status", &json!({
            "conversationSessionId":"rtc-new", "conversation":true,
            "audioAec":true, "audioAecPreparing":false})));
        assert!(session.capture_ready.load(Ordering::SeqCst));
        let pcm=vec![0; FRAME_BYTES];
        assert!(route_session_audio(&session, "audio/chunk", &json!({
            "conversationSessionId":"rtc-new", "aec":true, "vadSpeech":false,"data":BASE64.encode(&pcm)})));
        assert!(matches!(rx.try_recv(),Ok(Input::Pcm(data,Some(false))) if data==pcm));
        assert!(route_session_audio(&session, "audio/chunk", &json!({
            "conversationSessionId":"rtc-new", "aec":false,"data":BASE64.encode(&pcm)})));
        assert!(session.cancelled.load(Ordering::SeqCst));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn aec_failures_are_explained_and_legacy_firmware_stays_fail_closed() {
        use super::*;
        for (reason, expected) in [("init_failed","初始化失败"),("stream_reset_failed","初始化失败"),
            ("reference_overflow","参考缓冲溢出"),("processed_queue_overflow","处理缓冲溢出"),("dsp_too_slow","持续超时")] {
            let (session, _) = routing_session();
            session.capture_armed.store(true, Ordering::SeqCst);
            route_session_audio(&session,"audio/status",&json!({"conversationSessionId":"rtc-new",
                "conversation":true,"audioAec":false,"aecReason":reason}));
            assert!(session.cancelled.load(Ordering::SeqCst));
            assert!(session.status.lock().unwrap().error.contains(expected));
        }
        let (session, mut rx) = routing_session();
        session.capture_armed.store(true, Ordering::SeqCst);
        route_session_audio(&session,"audio/chunk",&json!({"aec":false}));
        assert!(!session.cancelled.load(Ordering::SeqCst));
        route_session_audio(&session,"audio/status",&json!({"conversation":true,"audioAec":true}));
        assert!(session.capture_ready.load(Ordering::SeqCst));
        route_session_audio(&session,"audio/chunk",&json!({"aec":true})); // Missing boolean VAD is invalid too.
        assert!(session.cancelled.load(Ordering::SeqCst));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn search_status_resets_between_turns_and_on_failure() {
        use super::*;
        let mut status=RealtimeChatStatus::default();
        update_search(&mut status,crate::web_search::Event::Started);
        assert_eq!(status.search_state,"searching");
        update_search(&mut status,crate::web_search::Event::Finished(crate::web_search::SearchResult{
            summary:"not sent to UI".into(), sources:vec![crate::web_search::Source{url:"https://example.com".into(),..Default::default()}],retrieved_at:"2026-09-22".into()
        }));
        assert_eq!(status.search_state,"completed");
        let snapshot=serde_json::to_string(&status).unwrap();
        assert!(!snapshot.contains("not sent to UI") && !snapshot.contains("https://example.com"));
        assert!(!snapshot.contains("searchSources") && !snapshot.contains("searchRetrievedAt"));
        clear_search(&mut status); assert!(status.search_state.is_empty());
        update_search(&mut status,crate::web_search::Event::Failed("未授权".into()));
        assert_eq!(status.search_state,"failed");
        update_search(&mut status,crate::web_search::Event::Started); assert!(status.search_error.is_empty());
    }
    use super::*;
    use crate::volcengine_asr::StreamingSpeechCommand;

    fn fake_barge_probe(frames: Vec<Vec<u8>>, texts: &[&str], keep_events_open: bool) -> (VerifiedSpeech, impl Sized) {
        let (recognizer, sink) = StreamingSpeechRecognizer::test_sink();
        let (tx, events) = mpsc::unbounded_channel();
        for (revision, text) in texts.iter().enumerate() {
            tx.send(StreamingSpeechEvent::Partial { revision: revision as u64, text: (*text).into(), confidence: None }).unwrap();
        }
        let mut stats = AsrCaptureStats::default();
        for frame in frames { recognizer.push_pcm(&frame).unwrap(); stats.observe(&frame); }
        (VerifiedSpeech { recognizer, events, stats, text: String::new(), reply_completed: false, terminal: false, no_transcript_stop: false },
            (sink, if keep_events_open { Some(tx) } else { None }))
    }

    /// Fake streaming ASR that emits nothing until `finish()` is requested, then
    /// returns exactly one Final. This is the hardware failure mode: a short
    /// command's transcript is never requested, so no Final can arrive before
    /// the confirmation deadline.
    fn fake_finish_driven_probe(frames: Vec<Vec<u8>>, final_text: &'static str) -> VerifiedSpeech {
        let (recognizer, mut sink) = StreamingSpeechRecognizer::test_sink();
        let (tx, events) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(command) = sink.recv().await {
                if matches!(command, StreamingSpeechCommand::Finish) {
                    let _ = tx.send(StreamingSpeechEvent::Final { revision: 1, text: final_text.into(), confidence: None });
                    break;
                }
            }
        });
        let mut stats = AsrCaptureStats::default();
        for frame in frames { recognizer.push_pcm(&frame).unwrap(); stats.observe(&frame); }
        VerifiedSpeech { recognizer, events, stats, text: String::new(), reply_completed: false, terminal: false, no_transcript_stop: false }
    }

    /// Fake ASR that emits one textual Partial, then an empty Final when
    /// `finish()` arrives. Any textual evidence must close the fallback.
    fn fake_partial_then_empty_final_probe(frames: Vec<Vec<u8>>, partial: &'static str) -> VerifiedSpeech {
        let (recognizer, mut sink) = StreamingSpeechRecognizer::test_sink();
        let (tx, events) = mpsc::unbounded_channel();
        let _ = tx.send(StreamingSpeechEvent::Partial { revision: 1, text: partial.into(), confidence: None });
        tokio::spawn(async move {
            while let Some(command) = sink.recv().await {
                if matches!(command, StreamingSpeechCommand::Finish) {
                    let _ = tx.send(StreamingSpeechEvent::Final { revision: 2, text: String::new(), confidence: None });
                    break;
                }
            }
        });
        let mut stats = AsrCaptureStats::default();
        for frame in frames { recognizer.push_pcm(&frame).unwrap(); stats.observe(&frame); }
        VerifiedSpeech { recognizer, events, stats, text: String::new(), reply_completed: false, terminal: false, no_transcript_stop: false }
    }

    /// Candidate speech followed by a known amount of trailing silence. The
    /// replay VAD needs 700 ms for its own End, so 400 ms exercises the new
    /// silence-window finish instead of the old VAD End path.
    fn short_barge_deferred(silence_frames: usize) -> VecDeque<Input> {
        (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(180), Some(true)))
            .chain((0..silence_frames).map(|_| Input::Pcm(frame(0), Some(false))))
            .collect()
    }

    /// 160 Hz sine: a periodic near-end vowel-like frame, unlike `frame()`'s
    /// alternating ±amplitude square wave (used as broadband/noise evidence).
    fn sine_frame(amplitude: i16, hz: f32) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_BYTES);
        for i in 0..(FRAME_BYTES / 2) {
            let t = i as f32 / 16_000.0;
            let v = (amplitude as f32 * (2.0 * std::f32::consts::PI * hz * t).sin()) as i16;
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn noise_frame(amplitude: i16) -> Vec<u8> {
        let mut seed = 0x2545_f491_u32;
        let mut out = Vec::with_capacity(FRAME_BYTES);
        for _ in 0..(FRAME_BYTES / 2) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            let v = (seed % (2 * amplitude as u32 + 1)) as i32 - amplitude as i32;
            out.extend_from_slice(&(v as i16).to_le_bytes());
        }
        out
    }

    /// A short periodic command candidate plus the trailing silence that asks
    /// the ASR for its Final.
    fn periodic_barge_deferred(voiced_frames: usize) -> VecDeque<Input> {
        (0..voiced_frames).map(|_| Input::Pcm(sine_frame(1200, 160.0), Some(true)))
            .chain((0..BARGE_FINISH_SILENCE_FRAMES).map(|_| Input::Pcm(frame(0), Some(false))))
            .collect()
    }

    /// A noise burst that passes the energy VAD exactly like a voice would,
    /// followed by the same trailing silence.
    fn noise_barge_deferred(voiced_frames: usize) -> VecDeque<Input> {
        (0..voiced_frames).map(|_| Input::Pcm(noise_frame(1500), Some(true)))
            .chain((0..BARGE_FINISH_SILENCE_FRAMES).map(|_| Input::Pcm(frame(0), Some(false))))
            .collect()
    }

    fn barge_session_in(state: &str) -> Arc<Session> {
        let (session, _) = routing_session();
        {
            let mut status = session.status.lock().unwrap();
            status.state = state.into();
            status.reply = "正在播放音乐".into();
        }
        Arc::new(session)
    }

    fn speaking_barge_session() -> Arc<Session> { barge_session_in("speaking") }

    #[test]
    fn barge_finish_policy_requires_voice_evidence_and_bounds_silence() {
        assert_eq!(barge_finish_reason(BARGE_FINISH_MIN_VOICED_FRAMES, BARGE_FINISH_SILENCE_FRAMES-1, false), None);
        assert_eq!(barge_finish_reason(BARGE_FINISH_MIN_VOICED_FRAMES, BARGE_FINISH_SILENCE_FRAMES, false), Some("silence_window"));
        assert_eq!(barge_finish_reason(BARGE_FINISH_MIN_VOICED_FRAMES-1, BARGE_FINISH_SILENCE_FRAMES, false), None);
        assert_eq!(barge_finish_reason(0, 0, true), Some("vad_end"));
        // The frame cap remains a separate caller-or term; windows_style_audio_burst... covers it.
    }

    #[test]
    fn short_explicit_barge_finalizes_after_silence_and_confirms() {
        tauri::async_runtime::block_on(async {
            for (text, final_text) in [("停", "停"), ("等等", "等等")] {
                let (_tx, mut rx) = mpsc::channel(8);
                let mut deferred = short_barge_deferred(BARGE_FINISH_SILENCE_FRAMES);
                let mut vad = Vad::new();
                vad.session = Some(speaking_barge_session());
                let reply = std::future::pending::<Result<(),String>>();
                let speech = timeout(Duration::from_secs(2), verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                    Ok(fake_finish_driven_probe(frames, final_text))
                })).await.expect("short barge must not wait for the 3 s deadline").unwrap()
                    .expect("short command must confirm after its Final transcript");
                assert_eq!(speech.text, final_text, "{text}");
                assert!(speech.terminal, "{text}");
                assert!(!speech.reply_completed, "{text}");
            }
        });
    }

    #[test]
    fn finalized_barge_noise_and_single_character_transcripts_still_reject() {
        tauri::async_runtime::block_on(async {
            for final_text in ["", "嗯"] {
                let (_tx, mut rx) = mpsc::channel(8);
                let mut deferred = short_barge_deferred(BARGE_FINISH_SILENCE_FRAMES);
                let mut vad = Vad::new();
                vad.session = Some(speaking_barge_session());
                let completed = Arc::new(AtomicBool::new(false));
                let flag = completed.clone();
                let reply = async move {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                };
                let result = timeout(Duration::from_secs(2), verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                    Ok(fake_finish_driven_probe(frames, final_text))
                })).await.expect("rejection path must stay bounded").unwrap();
                assert!(result.is_none(), "{final_text}");
                assert!(completed.load(Ordering::SeqCst), "{final_text}");
            }
        });
    }

    #[test]
    fn periodic_voice_check_separates_voiced_commands_from_noise() {
        // Vowel-like pitch in the normal speech range.
        assert!(periodic_voice_frame(&sine_frame(1200, 160.0)));
        assert!(periodic_voice_frame(&sine_frame(700, 240.0)));
        // Alternating ±amplitude is a Nyquist buzz, not a voice.
        assert!(!periodic_voice_frame(&frame(180)));
        // Broadband noise passes the same energy floor but is aperiodic.
        assert!(!periodic_voice_frame(&noise_frame(1500)));
        // Silence and weak near-end blips stay below the processed voice floor.
        assert!(!periodic_voice_frame(&frame(0)));
        assert!(!periodic_voice_frame(&sine_frame(100, 160.0)));
    }

    #[test]
    fn empty_final_short_command_interrupts_playback_without_a_transcript() {
        tauri::async_runtime::block_on(async {
            for voiced_frames in [8usize, 12, 24] {
                let (_tx, mut rx) = mpsc::channel(8);
                let mut deferred = periodic_barge_deferred(voiced_frames);
                let mut vad = Vad::new();
                vad.session = Some(speaking_barge_session());
                let reply = std::future::pending::<Result<(),String>>();
                let speech = timeout(Duration::from_secs(2), verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                    Ok(fake_finish_driven_probe(frames, ""))
                })).await.expect("empty Final fallback must stay bounded").unwrap()
                    .expect("a short voiced command must interrupt even without a transcript");
                assert!(speech.no_transcript_stop, "{voiced_frames}");
                assert_eq!(speech.text, "", "{voiced_frames}");
                assert!(!speech.reply_completed, "{voiced_frames}");
            }
        });
    }

    #[test]
    fn empty_final_ordinary_speech_noise_and_thinking_phase_still_reject() {
        use super::barge_policy::NO_TRANSCRIPT_MAX_VOICED_FRAMES;
        tauri::async_runtime::block_on(async {
            let cases = [
                ("long speech", periodic_barge_deferred(NO_TRANSCRIPT_MAX_VOICED_FRAMES + 6), "speaking"),
                ("noise", noise_barge_deferred(12), "speaking"),
                ("thinking", periodic_barge_deferred(12), "thinking"),
                ("listening", periodic_barge_deferred(12), "listening"),
            ];
            for (label, mut deferred, state) in cases {
                let (_tx, mut rx) = mpsc::channel(8);
                let mut vad = Vad::new();
                vad.session = Some(barge_session_in(state));
                let completed = Arc::new(AtomicBool::new(false));
                let flag = completed.clone();
                let reply = async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                };
                let result = timeout(Duration::from_secs(2), verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                    Ok(fake_finish_driven_probe(frames, ""))
                })).await.expect("rejection path must stay bounded").unwrap();
                assert!(result.is_none(), "{label}");
                assert!(completed.load(Ordering::SeqCst), "{label}");
            }
        });
    }

    #[test]
    fn empty_final_after_a_textual_partial_keeps_the_transcript_path_closed() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(8);
            let mut deferred = periodic_barge_deferred(12);
            let mut vad = Vad::new();
            vad.session = Some(speaking_barge_session());
            let completed = Arc::new(AtomicBool::new(false));
            let flag = completed.clone();
            let reply = async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                flag.store(true, Ordering::SeqCst);
                Ok(())
            };
            let result = timeout(Duration::from_secs(2), verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                Ok(fake_partial_then_empty_final_probe(frames, "嗯"))
            })).await.expect("rejection path must stay bounded").unwrap();
            assert!(result.is_none());
            assert!(completed.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn barge_in_needs_two_real_characters_or_explicit_stop() {
        for text in ["", "  ", "……！", "嗯", "啊，！", "a", "🙂🙂"] { assert!(!has_barge_in_text(text), "{text}"); }
        for text in ["停", " 停！ ", "停下", "等 一 下！", "你好", "OK", "换歌"] { assert!(has_barge_in_text(text), "{text}"); }
        for text in ["停", " 停！ "] { assert!(is_explicit_stop(text)); }
        for text in ["", "停下", "停止音乐", "等一下", "不停", "嗯"] { assert!(!is_explicit_stop(text)); }
    }

    #[test]
    fn delayed_tail_asr_does_not_abort_the_pending_task() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(1);
            let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(180),Some(true))).collect();
            let mut vad = Vad::new();
            vad.previous_user = "帮我添加买火车票的待办".into();
            vad.turn_started = Some(Instant::now());
            let mut guards = Vec::new();
            let completed = Arc::new(AtomicBool::new(false)); let flag = completed.clone();
            let reply = async move {
                tokio::time::sleep(Duration::from_millis(1200)).await;
                flag.store(true, Ordering::SeqCst); Ok(())
            };
            let result = verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                let (mut probe, guard) = fake_barge_probe(frames,&[],true); guards.push(guard);
                let (tx, events) = mpsc::unbounded_channel(); probe.events=events;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(950)).await;
                    let _ = tx.send(StreamingSpeechEvent::Final {revision:1,text:"的待办".into(),confidence:None});
                });
                Ok(probe)
            }).await.unwrap();
            assert!(result.is_none());
            assert!(completed.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn terminal_confirmation_is_handed_off_without_writing_a_closed_asr_stream() {
        tauri::async_runtime::block_on(async {
            for text in ["停", "换歌", "嗯", ""] {
                let (_tx, mut rx) = mpsc::channel(1);
                let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(180),Some(true))).collect();
                let mut vad = Vad::new(); let mut guards = Vec::new();
                let result = verify_barge_in_with(async { Ok(()) },&mut rx,&mut deferred,&mut vad,true,|frames| {
                    let (mut probe, guard) = fake_barge_probe(frames,&[],false); guards.push(guard);
                    let (tx, events) = mpsc::unbounded_channel(); probe.events=events;
                    tx.send(StreamingSpeechEvent::Final {revision:1,text:text.into(),confidence:None}).unwrap();
                    Ok(probe)
                }).await.unwrap();
                if has_barge_in_text(text) {
                    let speech = result.unwrap(); assert!(speech.terminal); assert_eq!(speech.text,text);
                } else { assert!(result.is_none()); }
            }
        });
    }

    #[test]
    fn failed_or_timed_out_probe_keeps_the_original_reply_running() {
        tauri::async_runtime::block_on(async {
            for failure in ["start", "stream", "timeout"] {
                let (_tx, mut rx) = mpsc::channel(1);
                let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(180),Some(true))).collect();
                let mut vad = Vad::new(); let mut guards = Vec::new();
                let completed = Arc::new(AtomicBool::new(false)); let flag=completed.clone();
                let reply = async move {flag.store(true,Ordering::SeqCst); Ok(())};
                let result = timeout(BARGE_CONFIRM_TIMEOUT+Duration::from_secs(1), verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                    if failure=="start" {return Err("test ASR unavailable".into());}
                    let (mut probe, guard)=fake_barge_probe(frames,&[],true); guards.push(guard);
                    if failure=="stream" {
                        let (tx, events)=mpsc::unbounded_channel(); probe.events=events;
                        tx.send(StreamingSpeechEvent::Error("test ASR closed".into())).unwrap();
                    }
                    Ok(probe)
                })).await.expect("candidate must be bounded").unwrap();
                assert!(result.is_none()); assert!(completed.load(Ordering::SeqCst));
            }
        });
    }

    #[test]
    fn noise_and_one_character_do_not_cancel_the_reply() {
        tauri::async_runtime::block_on(async {
            for texts in [vec![], vec!["嗯", "嗯，！"]] {
                let (_tx, mut rx) = mpsc::channel(32);
                let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(2500),Some(true))).collect();
                let mut vad = Vad::new(); let mut guards = Vec::new();
                let completed = Arc::new(AtomicBool::new(false)); let flag = completed.clone();
                let reply = async move { flag.store(true,Ordering::SeqCst); Ok(()) };
                let result = verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                    let (probe, guard) = fake_barge_probe(frames,&texts,false); guards.push(guard); Ok(probe)
                }).await.unwrap();
                assert!(result.is_none()); assert!(completed.load(Ordering::SeqCst));
            }
        });
    }

    #[test]
    fn final_two_characters_confirm_and_reuse_the_existing_asr_stream() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(32);
            let mut deferred = (0..PREROLL_FRAMES).map(|_| Input::Pcm(frame(180),Some(true))).collect();
            let mut vad = Vad::new(); let mut guards = Vec::new(); let mut starts = 0;
            let reply = std::future::pending::<Result<(),String>>();
            let speech = verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                starts += 1;
                let (mut probe, guard) = fake_barge_probe(frames,&[],true); guards.push(guard);
                let (tx, events)=mpsc::unbounded_channel(); probe.events=events;
                for text in ["等", "等，", "等下"] { tx.send(StreamingSpeechEvent::Partial{revision:1,text:text.into(),confidence:None}).unwrap(); }
                tx.send(StreamingSpeechEvent::Final{revision:2,text:"等下".into(),confidence:None}).unwrap();
                Ok(probe)
            }).await.unwrap().unwrap();
            assert_eq!(starts,1); assert_eq!(speech.text,"等下"); assert!(speech.terminal); assert!(!speech.reply_completed);
            assert_eq!(speech.stats.frames,PROCESSED_SPEECH_START_FRAMES);
            assert_eq!(deferred.len(),PREROLL_FRAMES-PROCESSED_SPEECH_START_FRAMES);
            assert!(vad.in_speech); assert!(speech.recognizer.push_pcm(&frame(180)).is_ok());
        });
    }

    #[test]
    fn provisional_two_words_do_not_abort_waiting_tool_reply() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx)=mpsc::channel(1);
            let mut deferred=(0..PROCESSED_SPEECH_START_FRAMES).map(|_|Input::Pcm(frame(200),Some(true))).collect();
            let mut vad=Vad::new(); let mut guards=Vec::new();
            let completed=Arc::new(AtomicBool::new(false)); let flag=completed.clone();
            let reply=async move { tokio::time::sleep(Duration::from_millis(30)).await; flag.store(true,Ordering::SeqCst); Ok(()) };
            let result=verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                let (probe,guard)=fake_barge_probe(frames,&["你好","谢谢"],false); guards.push(guard); Ok(probe)
            }).await.unwrap();
            assert!(result.is_none()); assert!(completed.load(Ordering::SeqCst));
        });
    }

    fn focus_fixture_with_origin(
        session_id: &str,
        updated_at: u64,
        origin: &str,
    ) -> crate::dsh_task::FocusSnapshot {
        crate::dsh_task::FocusSnapshot {
            session_id: session_id.into(),
            updated_at,
            seq: 1,
            source: "dsh-ui".into(),
            origin: origin.into(),
        }
    }

    fn focus_fixture(session_id: &str, updated_at: u64) -> crate::dsh_task::FocusSnapshot {
        focus_fixture_with_origin(session_id, updated_at, "user")
    }

    #[test]
    fn dsh_focus_selection_drives_auto_target_but_respects_explicit_p4_precedence() {
        use crate::dsh_task::FocusSampleStatus;
        let focus = focus_fixture("session-dsh", 100);
        // Auto P4 target: a capable DSH UI observer selection wins.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, true, true, Some(&focus), true, None),
            FocusDecision::UseFocus { focus: focus.clone(), cached: false }
        );
        // Explicit P4 newer/equal: P4 wins.
        assert_eq!(
            decide_focus_target("session-p4", 200, FocusSampleStatus::Ok, true, true, Some(&focus), true, None),
            FocusDecision::KeepP4
        );
        assert_eq!(
            decide_focus_target("session-p4", 100, FocusSampleStatus::Ok, true, true, Some(&focus), true, None),
            FocusDecision::KeepP4
        );
        // DSH UI newer: focus wins.
        assert_eq!(
            decide_focus_target("session-p4", 99, FocusSampleStatus::Ok, true, true, Some(&focus), true, None),
            FocusDecision::UseFocus { focus: focus.clone(), cached: false }
        );
        // Explicit P4 without a timestamp is never silently overridden.
        assert_eq!(
            decide_focus_target("session-p4", 0, FocusSampleStatus::Ok, true, true, Some(&focus), true, None),
            FocusDecision::KeepP4
        );
        // Capable observer with no main session is an explicit no-selection.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, true, true, None, true, None),
            FocusDecision::KeepP4
        );
    }

    #[test]
    fn dsh_focus_observer_capability_fails_closed_when_unavailable() {
        use crate::dsh_task::FocusSampleStatus;
        // F1: SSE-only/old client, or a new client with no usable services, is
        // not evidence that the focus observer can observe anything.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, false, false, None, true, None),
            FocusDecision::Unconfirmed
        );
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, true, false, None, true, None),
            FocusDecision::Unconfirmed
        );
        // Explicit P4 choice still wins; observer capability is irrelevant.
        assert_eq!(
            decide_focus_target("session-p4", 100, FocusSampleStatus::Ok, false, false, None, true, None),
            FocusDecision::KeepP4
        );
        // A last-confirmed selection avoids falling back to current.
        let cached = focus_fixture("session-cached", 50);
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, false, false, None, true, Some(&cached)),
            FocusDecision::UseFocus { focus: cached.clone(), cached: true }
        );
    }

    #[test]
    fn restored_focus_does_not_override_an_explicit_p4_selection() {
        use crate::dsh_task::FocusSampleStatus;
        let restored = focus_fixture_with_origin("session-restored", 500, "restore");
        // F3: renderer restore has a fresh timestamp but is not a user action.
        assert_eq!(
            decide_focus_target("session-p4", 100, FocusSampleStatus::Ok, true, true, Some(&restored), true, None),
            FocusDecision::KeepP4
        );
        // With no explicit P4 choice, the restored selection is still usable.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Ok, true, true, Some(&restored), true, None),
            FocusDecision::UseFocus { focus: restored.clone(), cached: false }
        );
        let user = focus_fixture_with_origin("session-restored", 500, "user");
        assert_eq!(
            decide_focus_target("session-p4", 100, FocusSampleStatus::Ok, true, true, Some(&user), true, None),
            FocusDecision::UseFocus { focus: user.clone(), cached: false }
        );
    }

    #[test]
    fn dsh_focus_unavailable_or_stale_state_fails_closed_not_current() {
        use crate::dsh_task::FocusSampleStatus;
        let cached = focus_fixture("session-cached", 50);
        // Focus service unreachable, no cache, auto P4 and reachable bridge:
        // dispatch must fail closed rather than use the current session.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Unavailable, false, false, None, true, None),
            FocusDecision::Unconfirmed
        );
        // Explicit P4 selection is still a valid user choice.
        assert_eq!(
            decide_focus_target("session-p4", 100, FocusSampleStatus::Unavailable, false, false, None, true, None),
            FocusDecision::KeepP4
        );
        // Bridge unreachable: dispatch will report bridge unavailable explicitly.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Unavailable, false, false, None, false, None),
            FocusDecision::KeepP4
        );
        // Last confirmed focus is used if it still wins over P4.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Unavailable, false, false, None, true, Some(&cached)),
            FocusDecision::UseFocus { focus: cached.clone(), cached: true }
        );
        // Older plugin without /focus preserves the historical P4/auto path.
        assert_eq!(
            decide_focus_target("auto", 0, FocusSampleStatus::Unsupported, false, false, None, true, None),
            FocusDecision::KeepP4
        );
    }

    #[test]
    fn focus_sync_is_scoped_to_the_dsh_agent_only() {
        assert!(follows_dsh_target("deepseek-harness", ""));
        assert!(follows_dsh_target("", "deepseek-harness"));
        assert!(!follows_dsh_target("codex", "deepseek-harness"));
        assert!(!follows_dsh_target("workbuddy", "deepseek-harness"));
        assert!(!follows_dsh_target("", "codex"));
    }

    #[test]
    fn dsh_focus_precedence_keeps_explicit_p4_newer_or_tied_selection() {
        // Auto/empty P4 target: any confirmed user DSH UI focus wins.
        assert!(focus_precedes_p4("auto", 0, 100, "user"));
        assert!(focus_precedes_p4("", 0, 100, "user"));
        assert!(focus_precedes_p4("", 0, 100, "restore"));
        // Explicit P4 target: DSH focus must be strictly newer and user-origin.
        assert!(focus_precedes_p4("session-p4", 100, 101, "user"));
        assert!(!focus_precedes_p4("session-p4", 100, 100, "user"));
        assert!(!focus_precedes_p4("session-p4", 100, 99, "user"));
        assert!(!focus_precedes_p4("session-p4", 100, 999, "restore"));
        // An explicit P4 target without a timestamp is never silently overridden.
        assert!(!focus_precedes_p4("session-p4", 0, 100, "user"));
    }

    #[test]
    fn resolve_focus_handles_cache_and_bridge_instance_changes() {
        use crate::dsh_task::{FocusSample, FocusSampleStatus, FocusSnapshot};
        let sample = |status, focus: Option<FocusSnapshot>, observer, instance: &str| FocusSample {
            status,
            focus,
            client_attached: observer,
            observer_attached: observer,
            observer_capable: observer,
            instance_id: instance.into(),
            bridge_reachable: true,
        };
        let focus = FocusSnapshot {
            session_id: "session-focus".into(),
            updated_at: 200,
            seq: 3,
            source: "dsh-ui".into(),
            origin: "user".into(),
        };
        let mut input: RealtimeChatStartInput = serde_json::from_value(serde_json::json!({
            "boardDeviceId":"board",
            "appearanceId":"pet-a",
            "displayName":"小西",
            "systemPrompt":"prompt",
            "targetAgentId":"deepseek-harness",
            "targetSessionId":"auto",
            "targetUpdatedAt": 0
        }))
        .unwrap();
        tauri::async_runtime::block_on(async {
            set_focus_sample_override(Some(sample(
                FocusSampleStatus::Ok,
                Some(focus.clone()),
                true,
                "bridge-a",
            )));
            resolve_dsh_focus_target(&mut input).await;
            assert_eq!(input.target_session_id, "session-focus");
            assert_eq!(input.target_updated_at, 200);
            assert!(!input.target_unconfirmed);

            // Focus becomes unreadable: the last confirmed cache is used.
            input.target_session_id = "auto".into();
            input.target_updated_at = 0;
            set_focus_sample_override(Some(sample(
                FocusSampleStatus::Unavailable,
                None,
                false,
                "bridge-a",
            )));
            resolve_dsh_focus_target(&mut input).await;
            assert_eq!(input.target_session_id, "session-focus");
            assert!(!input.target_unconfirmed);

            // A new bridge instance with an attached, capable observer and no
            // selection invalidates the old cache and keeps P4/auto.
            input.target_session_id = "auto".into();
            input.target_updated_at = 0;
            set_focus_sample_override(Some(sample(
                FocusSampleStatus::Ok,
                None,
                true,
                "bridge-b",
            )));
            resolve_dsh_focus_target(&mut input).await;
            assert_eq!(input.target_session_id, "auto");
            assert!(!input.target_unconfirmed);

            // No observer capability and no cache: fail closed for auto P4.
            input.target_session_id = "auto".into();
            input.target_updated_at = 0;
            set_focus_sample_override(Some(FocusSample {
                status: FocusSampleStatus::Ok,
                focus: None,
                client_attached: true,
                observer_attached: false,
                observer_capable: false,
                instance_id: "bridge-b".into(),
                bridge_reachable: true,
            }));
            resolve_dsh_focus_target(&mut input).await;
            assert!(input.target_unconfirmed);
            set_focus_sample_override(None);
        });
    }

    #[test]
    fn realtime_start_payload_accepts_the_p4_target_snapshot() {
        let input: RealtimeChatStartInput = serde_json::from_value(serde_json::json!({
            "boardDeviceId":"board",
            "appearanceId":"pet-a",
            "displayName":"小西",
            "systemPrompt":"prompt",
            "targetAgentId":"deepseek-harness",
            "targetSessionId":"session-selected"
        }))
        .unwrap();
        assert_eq!(input.target_agent_id, "deepseek-harness");
        assert_eq!(input.target_session_id, "session-selected");
        let legacy: RealtimeChatStartInput = serde_json::from_value(serde_json::json!({
            "boardDeviceId":"board",
            "appearanceId":"pet-a",
            "displayName":"小西",
            "systemPrompt":"prompt"
        }))
        .unwrap();
        assert!(legacy.target_agent_id.is_empty());
        assert!(legacy.target_session_id.is_empty());
    }

    #[test]
    fn windows_style_audio_burst_without_text_does_not_cancel_or_grow_unbounded() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(1);
            let mut deferred = (0..(PROCESSED_SPEECH_START_FRAMES+BARGE_CONFIRM_MAX_FRAMES))
                .map(|_| Input::Pcm(frame(2500),Some(true))).collect();
            let mut vad = Vad::new(); let mut guards = Vec::new(); let mut starts = 0;
            let result = verify_barge_in_with(async { Ok(()) },&mut rx,&mut deferred,&mut vad,true,|frames| {
                starts += 1;
                let (probe, guard) = fake_barge_probe(frames,&[],true); guards.push(guard); Ok(probe)
            }).await.unwrap();
            assert!(result.is_none()); assert_eq!(starts,1); assert!(deferred.is_empty());
        });
    }

    #[test]
    fn single_stop_interrupts_but_late_confirmation_does_not_recancel_a_finished_reply() {
        tauri::async_runtime::block_on(async {
            for finish_first in [false,true] {
                let (_tx, mut rx) = mpsc::channel(1);
                let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES).map(|_| Input::Pcm(frame(180),Some(true))).collect();
                let mut vad = Vad::new(); let mut guards = Vec::new();
                let reply = async move { if !finish_first { std::future::pending::<()>().await; } Ok(()) };
                let result = verify_barge_in_with(reply,&mut rx,&mut deferred,&mut vad,true,|frames| {
                    let (mut probe, guard) = fake_barge_probe(frames,&[],true); guards.push(guard);
                    let (tx, events) = mpsc::unbounded_channel(); probe.events = events;
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        tx.send(StreamingSpeechEvent::Partial {revision:1,text:"停！".into(),confidence:None}).unwrap();
                    });
                    Ok(probe)
                }).await.unwrap().unwrap();
                assert_eq!(result.text,"停！"); assert_eq!(result.reply_completed,finish_first);
            }
        });
    }

    #[test]
    fn listening_accepts_short_soft_speech_without_weakening_playback_gate() {
        let mut listening = Vad::new();
        let mut replying = Vad::new();
        for _ in 0..60 {
            listening.step_in_context(&frame(15), Some(false), VadContext::Listening);
            replying.step_with_hint(&frame(15), Some(false));
        }
        for _ in 0..(LISTENING_SPEECH_START_FRAMES-1) {
            assert!(matches!(listening.step_in_context(&frame(100), Some(true), VadContext::Listening), VadStep::Idle));
        }
        let VadStep::Start(preroll) = listening.step_in_context(&frame(100), Some(true), VadContext::Listening) else { panic!("soft short words must trigger while listening") };
        assert_eq!(preroll.len(), PREROLL_FRAMES);
        for _ in 0..50 { assert!(matches!(replying.step_with_hint(&frame(100), Some(true)), VadStep::Idle)); }
    }

    #[test]
    fn processed_vad_preserves_onset_and_tolerates_brief_consonant_dip() {
        let mut vad = Vad::new();
        for _ in 0..25 { vad.step_with_hint(&frame(20), Some(false)); }
        for _ in 0..4 { assert!(matches!(vad.step_with_hint(&frame(200), Some(true)), VadStep::Idle)); }
        for _ in 0..2 { assert!(matches!(vad.step_with_hint(&frame(65), Some(true)), VadStep::Idle)); }
        for _ in 0..3 { assert!(matches!(vad.step_with_hint(&frame(200), Some(true)), VadStep::Idle)); }
        let VadStep::Start(preroll) = vad.step_with_hint(&frame(200), Some(true)) else { panic!("speech-labelled energy dip must not reset the whole word") };
        assert_eq!(preroll.len(), PREROLL_FRAMES);
        assert_eq!(preroll[preroll.len()-10], frame(200));
    }

    #[test]
    fn sparse_speech_hints_and_playback_tail_do_not_trigger_soft_gate() {
        let mut vad = Vad::new();
        for _ in 0..20 {
            assert!(matches!(vad.step_with_hint(&frame(200), Some(true)), VadStep::Idle));
            for _ in 0..2 { assert!(matches!(vad.step_with_hint(&frame(20), Some(true)), VadStep::Idle)); }
        }
        vad.reset();
        vad.reply_tail_until = Some(Instant::now() + POST_SPEAK_GUARD);
        for _ in 0..20 {
            assert!(matches!(vad.step_in_context(&frame(100), Some(true), VadContext::Listening), VadStep::Idle));
        }
    }

    #[test]
    fn listening_rejects_low_level_hints_and_short_post_playback_noise() {
        let mut vad=Vad::new();
        for _ in 0..250 {
            assert!(matches!(vad.step_in_context(&frame(85),Some(true),VadContext::Listening),VadStep::Idle));
        }
        for _ in 0..3 {
            assert!(matches!(vad.step_in_context(&frame(114),Some(true),VadContext::Listening),VadStep::Idle));
        }
        assert!(matches!(vad.step_in_context(&frame(20),Some(false),VadContext::Listening),VadStep::Idle));
        assert!(!vad.in_speech);
    }

    #[test]
    fn processed_vad_rejects_quiet_speech_shaped_echo_but_accepts_near_speech() {
        let mut vad = Vad::new();
        for _ in 0..150 {assert!(matches!(vad.step_with_hint(&frame(15),Some(false)),VadStep::Idle));}
        for _ in 0..50 {assert!(matches!(vad.step_with_hint(&frame(40),Some(true)),VadStep::Idle));}
        let mut started = false;
        for _ in 0..PROCESSED_SPEECH_START_FRAMES {
            if matches!(vad.step_with_hint(&frame(180),Some(true)),VadStep::Start(_)) {started=true;}
        }
        assert!(started, "speech must remain interruptible, even at modest processed level");
        assert!(matches!(vad.step_with_hint(&frame(45),Some(true)),VadStep::Speech));
    }

    #[test]
    fn processed_vad_ignores_short_onset_bursts_without_muting_sustained_speech() {
        let mut vad=Vad::new();
        for _ in 0..3 {
            for _ in 0..(PROCESSED_SPEECH_START_FRAMES-1) {
                assert!(matches!(vad.step_with_hint(&frame(2500),Some(true)),VadStep::Idle));
            }
            assert!(matches!(vad.step_with_hint(&frame(20),Some(false)),VadStep::Idle));
        }
        for _ in 0..(PROCESSED_SPEECH_START_FRAMES-1) {
            assert!(matches!(vad.step_with_hint(&frame(180),Some(true)),VadStep::Idle));
        }
        assert!(matches!(vad.step_with_hint(&frame(180),Some(true)),VadStep::Start(_)));
    }

    #[test]
    fn duplex_uses_processed_vad_and_preserves_interruption_preroll() {
        tauri::async_runtime::block_on(async {
            let (tx, mut rx) = mpsc::channel(32);
            let frame=vec![20; FRAME_BYTES];
            for _ in 0..10 {tx.send(Input::Pcm(frame.clone(),Some(false))).await.unwrap();}
            for _ in 0..(PROCESSED_SPEECH_START_FRAMES+1) {tx.send(Input::Pcm(frame.clone(),Some(true))).await.unwrap();}
            let dropped=Arc::new(AtomicBool::new(false));
            struct MarkDrop(Arc<AtomicBool>);
            impl Drop for MarkDrop {fn drop(&mut self){self.0.store(true,Ordering::SeqCst);}}
            let marker=MarkDrop(dropped.clone());
            let response=async move {let _marker=marker; std::future::pending::<Result<(),String>>().await};
            let mut vad=Vad::new();
            let frames=listen_during_reply(response,&mut rx,&mut VecDeque::new(),&mut vad,true).await.unwrap().unwrap();
            assert!(dropped.load(Ordering::SeqCst),"old generation must be cancelled");
            assert_eq!(frames.len(),10+PROCESSED_SPEECH_START_FRAMES); assert!(vad.in_speech);
            assert!(matches!(rx.try_recv(),Ok(Input::Pcm(_,Some(true)))),"new speech must not be drained");
            assert!(matches!(vad.step_with_hint(&frame,Some(true)),VadStep::Speech));
        });
    }

    #[test]
    fn duplex_rejects_raw_pcm_and_keeps_preroll_when_reply_finishes() {
        tauri::async_runtime::block_on(async {
            let (tx,mut rx)=mpsc::channel(8); let mut vad=Vad::new();
            tx.send(Input::Pcm(vec![0;FRAME_BYTES],None)).await.unwrap();
            assert!(listen_during_reply(std::future::pending(),&mut rx,&mut VecDeque::new(),&mut vad,true).await.is_err());
            vad.step_with_hint(&vec![0;FRAME_BYTES],Some(false));
            assert!(listen_during_reply(async{Ok(())},&mut rx,&mut VecDeque::new(),&mut vad,true).await.unwrap().is_none());
            assert_eq!(vad.preroll.len(),1);
        });
    }

    #[test]
    fn slow_asr_final_drains_live_queue_and_preserves_followup_speech() {
        tauri::async_runtime::block_on(async {
            let (tx, mut rx) = mpsc::channel(128);
            let (done, finished) = tokio::sync::oneshot::channel();
            let sender = tokio::spawn(async move {
                // More than the ingress capacity; simulates a slow final reply.
                for _ in 0..300 { tx.send(Input::Pcm(frame(15), Some(false))).await.unwrap(); }
                for _ in 0..10 { tx.send(Input::Pcm(frame(800), Some(true))).await.unwrap(); }
                // Wait for the consumer to collect every frame, without closing ingress.
                while tx.capacity() != 128 { tokio::task::yield_now().await; }
                let _ = done.send(());
                tx
            });
            let mut deferred = VecDeque::new();
            let text = await_with_capture(async {finished.await.unwrap(); Ok("first sentence")}, &mut rx, &mut deferred, true).await.unwrap();
            let _tx = sender.await.unwrap();
            assert_eq!(text, "first sentence"); assert_eq!(deferred.len(),310);
            let mut vad = Vad::new();
            let frames = listen_during_reply(async {panic!("resumed speech must be evaluated before generating a reply")}, &mut rx, &mut deferred, &mut vad, true).await.unwrap().unwrap();
            assert_eq!(frames.len(),PREROLL_FRAMES);
            assert_eq!(deferred.len(),10-PROCESSED_SPEECH_START_FRAMES);
        });
    }

    #[test]
    fn final_capture_backlog_is_bounded_and_rejects_raw_audio() {
        tauri::async_runtime::block_on(async {
            let (tx,mut rx)=mpsc::channel(2);
            let mut deferred=VecDeque::new();
            tx.send(Input::Pcm(frame(0),None)).await.unwrap();
            assert!(await_with_capture(std::future::pending::<Result<(),String>>(),&mut rx,&mut deferred,true).await.is_err());
            for _ in 0..FINAL_CAPTURE_LIMIT {deferred.push_back(Input::Pcm(frame(0),Some(false)));}
            tx.send(Input::Pcm(frame(0),Some(false))).await.unwrap();
            assert!(await_with_capture(std::future::pending::<Result<(),String>>(),&mut rx,&mut deferred,true).await.is_err());
            assert_eq!(deferred.len(),FINAL_CAPTURE_LIMIT);
        });
    }

    #[test]
    fn playback_flush_invalidates_already_queued_pcm_and_subtitles() {
        let (tx,rx)=std_mpsc::channel();
        let player=Player{tx,epoch:Arc::new(AtomicU64::new(0))};
        player.send(PlayerCmd::Begin("old".into())).unwrap();
        player.send(PlayerCmd::Pcm(vec![1;640])).unwrap();
        player.send(PlayerCmd::Subtitle("old".into())).unwrap();
        player.send(PlayerCmd::Flush).unwrap();
        player.send(PlayerCmd::Begin("new".into())).unwrap();
        let epoch=player.epoch.load(Ordering::SeqCst);
        for _ in 0..3 {assert_ne!(rx.recv().unwrap().0,epoch);}
        assert!(matches!(rx.recv().unwrap(),(1,PlayerCmd::Flush)));
        assert!(matches!(rx.recv().unwrap(),(1,PlayerCmd::Begin(id)) if id=="new"));
    }

    #[test]
    fn transient_audio_write_stalls_are_dropped_within_a_bounded_budget() {
        use super::*;
        let mut recovery = PlaybackWriteRecovery::default();
        for failure in 1..=AUDIO_WRITE_MAX_CONSECUTIVE_FAILURES {
            assert_eq!(
                recovery.on_failure("Live serial write failed: timeout after 704 bytes (3 retries)"),
                PlaybackWriteAction::DropChunk,
                "consecutive failure {failure} must stay recoverable",
            );
            assert_eq!(recovery.consecutive_failures, failure);
        }
        // A recovered write clears the budget: occasional stalls never end a reply.
        recovery.on_success();
        assert_eq!(recovery.on_failure("Live serial write failed: would_block after 0 bytes (0 retries)"),
            PlaybackWriteAction::DropChunk);
        // A sustained stall still fails closed instead of dropping audio forever.
        for _ in 0..=AUDIO_WRITE_MAX_CONSECUTIVE_FAILURES {
            recovery.on_failure("Live serial write failed: timeout after 0 bytes (8 retries)");
        }
        assert_eq!(recovery.on_failure("Live serial write failed: timeout after 0 bytes (8 retries)"),
            PlaybackWriteAction::Stop);
    }

    #[test]
    fn permanent_or_unknown_audio_write_errors_fail_closed() {
        use super::*;
        for error in [
            "Live serial write failed: broken_pipe after 128 bytes (0 retries)",
            "Live serial write failed: not_connected after 0 bytes (0 retries)",
            "Live serial write failed: permission_denied after 0 bytes (0 retries)",
            "Live serial write failed: write_zero after 0 bytes (0 retries)",
            "Live serial write failed: write_progress_unknown after 64 bytes (0 retries)",
            "Live serial write failed: io_error after 64 bytes (0 retries)",
            "Connection lost",
            "Not connected",
            "设备音频传输失败",
        ] {
            let mut recovery = PlaybackWriteRecovery::default();
            assert_eq!(recovery.on_failure(error), PlaybackWriteAction::Stop, "{error}");
        }
        // A transient marker must not override a permanent classification.
        let mut recovery = PlaybackWriteRecovery::default();
        assert_eq!(recovery.on_failure("write_progress_unknown timeout"), PlaybackWriteAction::Stop);
    }

    #[test]
    fn late_tts_callback_after_flush_cannot_replay_the_previous_sentence() {
        let (tx, rx) = std_mpsc::channel();
        let player = Player { tx, epoch: Arc::new(AtomicU64::new(0)) };
        // speak_one_logged() captures the epoch before synthesis starts, so a
        // callback that arrives after a flush keeps the pre-flush epoch.
        let synthesis_epoch = player.epoch();
        player.send(PlayerCmd::Begin("old-turn".into())).unwrap();
        player.send(PlayerCmd::Flush).unwrap();
        player.send_at(synthesis_epoch, PlayerCmd::Subtitle("旧句子".into())).unwrap();
        player.send_at(synthesis_epoch, PlayerCmd::Pcm(vec![7; PLAY_CHUNK_BYTES])).unwrap();
        player.send(PlayerCmd::Begin("new-turn".into())).unwrap();
        let current = player.epoch();
        assert_eq!(current, synthesis_epoch + 1);
        let mut stale = Vec::new();
        let mut fresh = Vec::new();
        while let Ok((epoch, cmd)) = rx.try_recv() {
            if epoch == current { fresh.push(cmd); } else { stale.push((epoch, cmd)); }
        }
        // The player thread skips every command whose epoch is not current:
        // the old sentence's late PCM/subtitle can never repaint the new bubble.
        assert_eq!(stale.len(), 3);
        assert!(stale.iter().all(|(epoch, _)| *epoch == synthesis_epoch));
        assert_eq!(fresh.len(), 2);
        assert!(matches!(fresh[0], PlayerCmd::Flush));
        assert!(matches!(&fresh[1], PlayerCmd::Begin(id) if id == "new-turn"));
    }

    #[test]
    fn reply_that_teaches_the_stop_word_does_not_self_interrupt_playback() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(4);
            let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES)
                .map(|_| Input::Pcm(frame(180), Some(true))).collect();
            let mut vad = Vad::new();
            let (session, _) = routing_session();
            {
                let mut status = session.status.lock().unwrap();
                status.state = "speaking".into();
                status.reply = "不想听的话，你可以说停来结束".into();
            }
            vad.session = Some(Arc::new(session));
            let mut guards = Vec::new();
            let completed = Arc::new(AtomicBool::new(false));
            let flag = completed.clone();
            let reply = async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                flag.store(true, Ordering::SeqCst);
                Ok(())
            };
            let result = verify_barge_in_with(reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                let (mut probe, guard) = fake_barge_probe(frames, &[], true); guards.push(guard);
                let (tx, events) = mpsc::unbounded_channel(); probe.events = events;
                tx.send(StreamingSpeechEvent::Final { revision: 1, text: "停".into(), confidence: None }).unwrap();
                Ok(probe)
            }).await.unwrap();
            assert!(result.is_none(), "an echoed stop word must not cancel the reply");
            assert!(completed.load(Ordering::SeqCst), "the original reply must keep running");
        });
    }

    #[test]
    fn explicit_command_with_voice_evidence_interrupts_playback_immediately() {
        tauri::async_runtime::block_on(async {
            let (_tx, mut rx) = mpsc::channel(4);
            let mut deferred = (0..PROCESSED_SPEECH_START_FRAMES)
                .map(|_| Input::Pcm(frame(180), Some(true))).collect();
            let mut vad = Vad::new();
            let (session, _) = routing_session();
            {
                let mut status = session.status.lock().unwrap();
                status.state = "speaking".into();
                status.reply = "正在播放音乐".into();
            }
            vad.session = Some(Arc::new(session));
            let mut guards = Vec::new();
            let completed = Arc::new(AtomicBool::new(false));
            let flag = completed.clone();
            let reply = async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                flag.store(true, Ordering::SeqCst);
                Ok(())
            };
            let speech = timeout(Duration::from_secs(2), verify_barge_in_with(
                reply, &mut rx, &mut deferred, &mut vad, true, |frames| {
                    let (mut probe, guard) = fake_barge_probe(frames, &[], true); guards.push(guard);
                    let (tx, events) = mpsc::unbounded_channel(); probe.events = events;
                    // Partial transcript only: the command must not wait for a final.
                    tx.send(StreamingSpeechEvent::Partial { revision: 1, text: "换歌".into(), confidence: None }).unwrap();
                    Ok(probe)
                })).await.expect("explicit command must confirm without a terminal transcript")
                .unwrap().expect("explicit command must interrupt");
            assert_eq!(speech.text, "换歌");
            assert!(!speech.reply_completed);
            assert!(!completed.load(Ordering::SeqCst), "confirming the command drops the old reply");
        });
    }

    #[test]
    fn saved_persona_is_targeted_latest_wins_and_survives_audio_drain() {
        tauri::async_runtime::block_on(async {
            let (tx, mut rx) = mpsc::channel(128);
            let session = Session { id:"rtc-test".into(), board_device_id:"board".into(), cancelled:AtomicBool::new(false), duplex:AtomicBool::new(false),
                capture_armed:AtomicBool::new(false), capture_ready:AtomicBool::new(false),
                stop_notify:tokio::sync::Notify::new(), input:tx,
                status:Mutex::new(RealtimeChatStatus { active:true, appearance_id:"pet-a".into(), ..Default::default() }),
                completed_playback:Mutex::new(String::new()), pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits()) };
            let mut current = RealtimeChatStartInput { board_device_id:"board".into(), appearance_id:"pet-a".into(),
                display_name:"old".into(), system_prompt:"old prompt".into(), greeting:"hello".into(), voice:VoiceSpec::default(),
                target_agent_id:String::new(), target_session_id:String::new(), target_updated_at:0, target_unconfirmed:false };
            let mut next = current.clone(); next.appearance_id = "pet-b".into();
            assert!(!queue_persona_update(&session, next.clone()).unwrap());
            next.appearance_id = "pet-a".into(); next.display_name = "new".into();
            next.voice.speaker = "new-speaker".into(); next.voice.speed = 1.2; next.voice.volume = 0.8;
            assert!(queue_persona_update(&session, next.clone()).unwrap());
            next.system_prompt = "latest prompt".into(); next.board_device_id.clear();
            assert!(queue_persona_update(&session, next).unwrap());
            session.input.try_send(Input::Pcm(vec![0; FRAME_BYTES], None)).unwrap(); drain_pending(&mut rx);
            let mut tts = DoubaoTtsClient::new(crate::doubao_tts::DoubaoTtsConfig::new("test", "old", "", 1.0, ""));
            assert!(apply_pending_persona_with(&session, &mut current, &mut tts, |voice| {
                assert_eq!(voice.speaker, "new-speaker"); assert_eq!(voice.speed, 1.2); assert_eq!(voice.volume, 0.8);
                Ok(crate::doubao_tts::DoubaoTtsConfig::new("test", &voice.speaker, "", voice.speed(), ""))
            }).await.unwrap());
            assert_eq!(current.system_prompt, "latest prompt"); assert_eq!(current.display_name, "new");
            assert_eq!(current.board_device_id, "board"); assert_eq!(current.appearance_id, "pet-a");
            let mut prompt_only = current.clone(); prompt_only.system_prompt = "next prompt".into(); prompt_only.voice.volume = 0.6;
            queue_persona_update(&session, prompt_only).unwrap();
            assert!(apply_pending_persona_with(&session, &mut current, &mut tts, |_| panic!("unchanged voice must reuse TTS")).await.unwrap());
            assert_eq!(current.voice.volume, 0.6);
            assert!(!apply_pending_persona_with(&session, &mut current, &mut tts, |_| panic!("no pending update")).await.unwrap());
            session.cancelled.store(true, Ordering::SeqCst);
            assert!(!queue_persona_update(&session, current).unwrap());
        });
    }

    #[test]
    fn displayed_asr_partial_is_preserved_when_final_stream_closes() {
        tauri::async_runtime::block_on(async {
            let (tx, mut rx) = mpsc::unbounded_channel();
            drop(tx);
            assert_eq!(await_final_text(&mut rx, "已经显示的语音".into()).await.unwrap(), "已经显示的语音");
            let (tx, mut rx) = mpsc::unbounded_channel();
            tx.send(StreamingSpeechEvent::Partial { revision: 1, text: " ".into(), confidence: None }).unwrap();
            tx.send(StreamingSpeechEvent::Final { revision: 2, text: "".into(), confidence: None }).unwrap();
            assert_eq!(await_final_text(&mut rx, "停下".into()).await.unwrap(), "停下");
            let (tx, mut rx) = mpsc::unbounded_channel();
            tx.send(StreamingSpeechEvent::Error("service failed".into())).unwrap();
            drop(tx);
            assert!(await_final_text(&mut rx, "partial".into()).await.is_err());
        });
    }

    #[test]
    fn cancellation_survives_microphone_queue_drain() {
        let (tx, mut rx) = mpsc::channel(128);
        let session = Session { id:"rtc-test".into(), board_device_id:"test".into(), cancelled:AtomicBool::new(false), duplex:AtomicBool::new(false),
            capture_armed:AtomicBool::new(false), capture_ready:AtomicBool::new(false),
            stop_notify:tokio::sync::Notify::new(), input:tx, status:Mutex::new(RealtimeChatStatus::default()), completed_playback:Mutex::new(String::new()), pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits()) };
        session.input.try_send(Input::Pcm(vec![0; FRAME_BYTES], None)).unwrap();
        request_stop(&session, "key");
        drain_pending(&mut rx);
        assert!(session.cancelled.load(Ordering::SeqCst));
        tauri::async_runtime::block_on(async { timeout(Duration::from_millis(100), session.stop_notify.notified()).await.unwrap(); });
    }

    #[test]
    fn asr_timeout_discards_incomplete_round_and_next_utterance_succeeds() {
        tauri::async_runtime::block_on(async {
            let (old_tx, mut old_rx)=mpsc::unbounded_channel();
            old_tx.send(StreamingSpeechEvent::Partial {revision:1,text:"打开".into(),confidence:None}).unwrap();
            let error=await_final_text_with_timeout(&mut old_rx,String::new(),Duration::from_millis(5)).await.unwrap_err();
            assert!(recoverable_asr_error(&error));
            drop(old_rx);
            assert!(old_tx.send(StreamingSpeechEvent::Final {revision:2,text:"过期指令".into(),confidence:None}).is_err());
            let mut vad=Vad::new(); vad.in_speech=true; vad.speech_run=10;
            let mut latest="打开".to_string(); let mut displayed=latest.clone();
            let mut final_text=Some("旧内容".into()); let mut terminal=true;
            let mut pending=VecDeque::from([Input::Pcm(vec![0;FRAME_BYTES],None)]);
            reset_timed_out_asr(&mut vad,&mut latest,&mut displayed,&mut final_text,&mut terminal,&mut pending);
            assert!(!vad.in_speech && vad.speech_run==0 && pending.is_empty());
            assert!(latest.is_empty() && displayed.is_empty() && final_text.is_none() && !terminal);
            let (tx,mut rx)=mpsc::unbounded_channel();
            tx.send(StreamingSpeechEvent::Final {revision:1,text:"帮我打开36kr".into(),confidence:None}).unwrap();
            assert_eq!(await_final_text_with_timeout(&mut rx,String::new(),Duration::from_millis(50)).await.unwrap(),"帮我打开36kr");
        });
    }

    #[test]
    fn asr_recovery_never_masks_configuration_or_authentication_errors() {
        assert!(recoverable_asr_error("语音识别结果超时"));
        assert!(recoverable_asr_error("ASR connection timeout"));
        for error in ["401 鉴权失败", "403 授权失败", "请配置 API Key", "协议错误", "401 request timeout"] {
            assert!(!recoverable_asr_error(error),"{error}");
        }
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "explicit cloud integration test; requires PET_REALTIME_SETTINGS_DIR"]
    fn live_voice_pipeline() {
        let dir = std::path::PathBuf::from(std::env::var("PET_REALTIME_SETTINGS_DIR").unwrap());
        voice_chat_settings::configure_storage_dir(dir.clone()).unwrap();
        crate::llm_network::configure_storage_dir(dir.clone()).unwrap();
        crate::volcengine_asr::configure_storage_dir(dir).unwrap();
        tauri::async_runtime::block_on(async {
            let cfg = voice_chat_settings::tts_config("zh_female_vv_uranus_bigtts", "", 1.0).unwrap();
            let mut tts = DoubaoTtsClient::new(cfg);
            let mut history=Vec::new();
            for turn in 0..3 {
            let mut question = Vec::new();
            tts.synthesize("你好，请告诉我你是谁。", |pcm| question.extend_from_slice(pcm)).await.unwrap();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let rec = StreamingSpeechRecognizer::start(move |e| { let _ = tx.send(e); }).unwrap();
            for chunk in question.chunks(FRAME_BYTES) {
                rec.push_pcm(chunk).unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            rec.finish().unwrap();
            let text = await_final_text(&mut rx, String::new()).await.unwrap();
            assert!(text.contains("你") && text.contains("谁"), "ASR did not recognize the synthetic test sentence");
            let reply = persona_llm::stream_chat(&voice_chat_settings::llm_config().unwrap(), "你是桌面宠物小西。用中文一句话回答，不超过二十字。", &history, &text, |_| {}).await.unwrap();
            assert!(!reply.trim().is_empty());
            let mut bytes = 0;
            tts.synthesize(&reply, |pcm| bytes += pcm.len()).await.unwrap();
            assert!(bytes > 6400);
            history.push(ChatTurn{role:"user".into(),content:text.clone()});
            history.push(ChatTurn{role:"assistant".into(),content:reply.clone()});
            // Cancel an actual streaming TTS response after the first PCM arrives.
            // Synthetic near-end PCM exercises the production VAD/cancellation
            // boundary; the separate hardware test covers actual acoustic input.
            let (tx,mut rx)=mpsc::channel(32);
            let ready=Arc::new(tokio::sync::Notify::new());
            let started=ready.clone();
            let injection=tokio::spawn(async move {
                started.notified().await;
                for _ in 0..PROCESSED_SPEECH_START_FRAMES {
                    tx.send(Input::Pcm(frame(1000),Some(true))).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                tx
            });
            let response=async {
                tts.synthesize("这是一段用于检验打断的较长回复。用户可以在我说话的时候提出新的问题。我会停止旧的回复，继续倾听新的内容。", |_|ready.notify_one()).await?;
                std::future::pending::<Result<(),String>>().await
            };
            let mut vad=Vad::new();
            let interruption=timeout(Duration::from_secs(30),listen_during_reply(response,&mut rx,&mut VecDeque::new(),&mut vad,true)).await.unwrap().unwrap();
            assert!(interruption.is_some());
            tts.interrupt();
            injection.await.unwrap();
            // Reuse the client after interruption; stale audio must not poison it.
            let mut resumed=0;
            tts.synthesize("好的，请继续。", |pcm|resumed+=pcm.len()).await.unwrap();
            assert!(resumed>6400);
            println!("PASS cloud turn={}: TTS → ASR → LLM → TTS; interruption and next TTS recovered; ASR chars={}, reply chars={}, PCM bytes={bytes}",turn+1,text.chars().count(),reply.chars().count());
            }
            tts.reset().await;
        });
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "exclusive P4 port; Mac speaker emits synthetic speech into the real device microphone; cloud ASR only"]
    fn live_device_near_end_asr() {
        let dir = std::path::PathBuf::from(std::env::var("PET_REALTIME_SETTINGS_DIR").unwrap());
        voice_chat_settings::configure_storage_dir(dir.clone()).unwrap();
        crate::llm_network::configure_storage_dir(dir.clone()).unwrap();
        crate::volcengine_asr::configure_storage_dir(dir).unwrap();
        // Generated stimulus only. Microphone PCM is never written to disk.
        let pcm = tauri::async_runtime::block_on(async {
            let cfg = voice_chat_settings::tts_config("zh_female_vv_uranus_bigtts", "", 1.0).unwrap();
            let mut tts = DoubaoTtsClient::new(cfg);
            let mut pcm = Vec::new();
            tts.synthesize("你好，请告诉我你是谁。", |data| pcm.extend_from_slice(data)).await.unwrap();
            tts.reset().await;
            pcm
        });
        let temp = tempfile::tempdir().unwrap();
        let wav = temp.path().join("synthetic-near-end.wav");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF"); bytes.extend_from_slice(&(36u32 + pcm.len() as u32).to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt "); bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes()); bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&16000u32.to_le_bytes()); bytes.extend_from_slice(&32000u32.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes()); bytes.extend_from_slice(&16u16.to_le_bytes());
        bytes.extend_from_slice(b"data"); bytes.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&pcm); std::fs::write(&wav, bytes).unwrap();
        let usb = UsbSerialManager::new();
        let (tx, events) = std_mpsc::channel();
        usb.connect(&std::env::var("P4_SERIAL_PORT").unwrap(), move |topic,payload| {let _=tx.send((topic,payload));}).unwrap();
        let board = usb.status().board_device_id;
        assert_eq!(board, std::env::var("P4_EXPECTED_BOARD_ID").unwrap());
        assert_eq!(usb.status().capabilities["features"]["audioFullDuplex"], true);
        struct Cleanup(UsbSerialManager, String, Option<std::process::Child>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if let Some(child)=self.2.as_mut() {let _=child.kill(); let _=child.wait();}
                let _=self.0.send_to_board(&self.1,"audio/conversation",&json!({"enabled":false}));
                self.0.disconnect();
            }
        }
        let mut cleanup = Cleanup(usb.clone(), board.clone(), None);
        usb.send_to_board(&board,"audio/conversation",&json!({"enabled":true,"halfDuplex":false,"sessionId":"rtc-near-test"})).unwrap();
        let began = Instant::now();
        let mut vad = Vad::new();
        let mut rec: Option<StreamingSpeechRecognizer> = None;
        let (asr_tx, mut asr_events) = mpsc::unbounded_channel();
        let compare_cache = std::env::var("P4_COMPARE_CACHE").as_deref() == Ok("1");
        let capture_all = std::env::var("P4_NEAR_CAPTURE_ALL").as_deref() == Ok("1");
        let mut short_audio = Vec::new();
        let mut ended = false;
        let mut frames = 0;
        let mut hints = 0;
        let mut peak = 0f32;
        while began.elapsed() < Duration::from_secs(18) {
            if cleanup.2.is_none() && began.elapsed() > Duration::from_secs(2) {
                if capture_all {
                    let tx=asr_tx.clone();
                    rec=Some(StreamingSpeechRecognizer::start(move |e| {let _=tx.send(e);}).unwrap());
                }
                cleanup.2=Some(std::process::Command::new("/usr/bin/afplay").arg(&wav).spawn().unwrap());
            }
            let Ok((topic,p))=events.recv_timeout(Duration::from_millis(30)) else {continue};
            if topic=="audio/error" {panic!("device audio failure");}
            if topic!="audio/chunk" {continue;}
            assert_eq!(p["aec"],true);
            let data=BASE64.decode(p["data"].as_str().unwrap()).unwrap();
            frames+=1; hints+=usize::from(p["vadSpeech"]==true); peak=peak.max(Vad::rms(&data));
            if capture_all {
                if let Some(rec)=rec.as_ref() {
                    rec.push_pcm(&data).unwrap();
                    if began.elapsed()>Duration::from_millis(3000+pcm.len() as u64*1000/32000) {
                        rec.finish().unwrap();ended=true;break;
                    }
                }
                continue;
            }
            match vad.step_in_context(&data,p["vadSpeech"].as_bool(),VadContext::Listening) {
                VadStep::Start(preroll) => {
                    println!("near-end VAD start frame={frames} preroll={} rms={:.1}",preroll.len(),Vad::rms(&data));
                    if compare_cache {
                        for frame in preroll.iter().skip(preroll.len().saturating_sub(15)) {short_audio.extend_from_slice(frame);}
                    }
                    let asr_tx=asr_tx.clone();
                    let next=StreamingSpeechRecognizer::start(move |e| {let _=asr_tx.send(e);}).unwrap();
                    for frame in preroll {next.push_pcm(&frame).unwrap();}
                    rec=Some(next);
                }
                VadStep::Speech => {
                    if compare_cache {short_audio.extend_from_slice(&data);}
                    if let Some(rec)=rec.as_ref() {rec.push_pcm(&data).unwrap();}
                }
                VadStep::End => {if let Some(rec)=rec.as_ref() {
                    if compare_cache {short_audio.extend_from_slice(&data);}
                    rec.push_pcm(&data).unwrap(); rec.finish().unwrap(); ended=true; break;
                }}
                VadStep::Idle => {}
            }
        }
        // Stop capture before waiting for the cloud; this test never executes tools/LLM.
        usb.send_to_board(&board,"audio/conversation",&json!({"enabled":false})).unwrap();
        println!("near-end physical capture: frames={frames} hint_frames={hints} peak_rms={peak:.1} ended={ended}");
        assert!(ended,"external test speech did not produce a complete utterance; inspect physical output route/level and VAD metrics");
        let text=tauri::async_runtime::block_on(await_final_text(&mut asr_events,String::new())).unwrap();
        println!("near-end ASR chars={} expected_phrase={}",text.chars().count(),text.contains("你是谁"));
        if compare_cache {
            let short = tauri::async_runtime::block_on(async {
                let (tx,mut rx)=mpsc::unbounded_channel();
                let rec=StreamingSpeechRecognizer::start(move |e| {let _=tx.send(e);}).unwrap();
                for frame in short_audio.chunks(FRAME_BYTES) {
                    rec.push_pcm(frame).unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                rec.finish().unwrap();
                await_final_text(&mut rx,String::new()).await.unwrap()
            });
            let normalized = |s: &str| s.chars().filter(|c| c.is_alphanumeric()).collect::<String>();
            let expected = "你好请告诉我你是谁";
            println!("cache paired same capture: ms=300 chars={} exact={} keyword={}; ms=600 chars={} exact={} keyword={}",
                short.chars().count(),normalized(&short)==expected,short.contains("你是谁"),
                text.chars().count(),normalized(&text)==expected,text.contains("你是谁"));
        }
        assert!(text.contains("你是谁"),"captured speech did not retain the expected synthetic phrase");
        drop(rec);
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "requires exclusive P4_SERIAL_PORT, saved credentials; plays a short test sentence"]
    fn live_device_audio() {
        let dir = std::path::PathBuf::from(std::env::var("PET_REALTIME_SETTINGS_DIR").unwrap());
        voice_chat_settings::configure_storage_dir(dir.clone()).unwrap();
        crate::llm_network::configure_storage_dir(dir.clone()).unwrap();
        crate::volcengine_asr::configure_storage_dir(dir.clone()).unwrap();
        diagnostics::configure(&dir).unwrap();
        let port = std::env::var("P4_SERIAL_PORT").unwrap();
        let usb = UsbSerialManager::new();
        let (events_tx, events_rx) = std_mpsc::channel();
        usb.connect(&port, move |topic, payload| { let _ = events_tx.send((topic, payload)); }).unwrap();
        let board = usb.status().board_device_id;
        assert!(!board.is_empty());
        assert_eq!(board,std::env::var("P4_EXPECTED_BOARD_ID").unwrap());
        assert_eq!(usb.status().runtime, "esp-p4");
        assert!(["audioAec","audioVad","audioFullDuplex"].iter().all(|key| usb.status().capabilities["features"][*key]==true));
        struct Cleanup(UsbSerialManager, String);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = self.0.send_to_board(&self.1, "audio/conversation", &json!({"enabled":false}));
                let _ = self.0.send_to_board(&self.1, "ui/conversation", &json!({"state":"ended"}));
                self.0.disconnect();
            }
        }
        let _cleanup = Cleanup(usb.clone(), board.clone());
        let diag = usb.query_diagnostics(&board).unwrap();
        assert_eq!(diag["runtime"]["audioReady"], true);
        assert_eq!(diag["runtime"]["audioPlaybackReady"], true);
        let id = format!("rtc-hil-{}", now_ms());
        let (input, _) = mpsc::channel(128);
        let session = Arc::new(Session { id:id.clone(), board_device_id:board.clone(), cancelled:AtomicBool::new(false), duplex:AtomicBool::new(true),
            capture_armed:AtomicBool::new(true), capture_ready:AtomicBool::new(false),
            stop_notify:tokio::sync::Notify::new(), input, status:Mutex::new(RealtimeChatStatus::default()), completed_playback:Mutex::new(String::new()), pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits()) });
        let player = spawn_player(usb.clone(), board.clone(), session.clone());
        usb.send_to_board(&board,"audio/conversation",&json!({"enabled":true,"halfDuplex":false,"sessionId":id})).unwrap();
        // Match begin_capture: playback must not race DSP handle recreation.
        // Hardware tests written before the readiness handshake need the same
        // gate as production, not a greeting sent immediately after enable.
        let ready_until=Instant::now()+Duration::from_secs(3);
        let mut prepared=false;
        while Instant::now()<ready_until {
            if let Ok((topic,payload))=events_rx.recv_timeout(Duration::from_millis(30)) {
                if topic=="audio/status" && payload["conversationSessionId"]==id
                    && payload["conversation"]==true && payload["audioAec"]==true
                    && payload["audioAecPreparing"]==false {prepared=true;break;}
            }
        }
        assert!(prepared,"DSP did not become ready before test playback");
        tauri::async_runtime::block_on(async {
            let cfg = voice_chat_settings::tts_config("zh_male_naiqimengwa_mars_bigtts", "", 1.05).unwrap();
            let mut tts = DoubaoTtsClient::new(cfg);
            hud(&usb, &board, &id, "preparing", "音频测试", "");
            player.send(PlayerCmd::Begin(id.clone())).unwrap();
            hud(&usb, &board, &id, "speaking", "音频测试", "");
            speak_one(&mut tts, &player, "你好，设备语音播放测试。请稍等。" ).await.unwrap();
            wait_playback(&player).await;
            tts.reset().await;
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut played = false;
        let mut vad=Vad::new(); let mut false_starts=0; let mut duplex_frames=0;
        while Instant::now() < deadline {
            if let Ok((topic, payload)) = events_rx.recv_timeout(Duration::from_millis(100)) {
                if topic=="audio/chunk" {
                    assert_eq!(payload["aec"],true);
                    let pcm=BASE64.decode(payload["data"].as_str().unwrap()).unwrap();
                    duplex_frames+=1;
                    // Match the production reply/barge-in gate explicitly.
                    if matches!(vad.step_in_context(&pcm,payload["vadSpeech"].as_bool(),VadContext::Reply),VadStep::Start(_)) {
                        false_starts+=1;
                        println!("reply echo candidate frame={duplex_frames} rms={:.1}",Vad::rms(&pcm));
                    }
                }
                if topic == "audio/diagnostic" {
                    diagnostics::record(&id, "device", json!({"stage":payload["event"],"bytes":payload["bytes"],"ok":payload["ok"]}));
                    if payload["event"] == "playback_completed" { played = true; break; }
                }
            }
        }
        assert!(played, "device did not confirm playback completion");
        assert!(duplex_frames>50,"full-duplex capture missing during actual cloud TTS playback");
        assert_eq!(false_starts,0,"actual cloud TTS echoed into production VAD");
        usb.send_to_board(&board, "audio/query", &json!({})).unwrap();
        hud(&usb, &board, &id, "listening", "收音测试", "");
        let deadline = Instant::now() + Duration::from_secs(4);
        let mut bytes = 0;
        let mut volume_confirmed = false;
        let mut peak_rms = 0f32;
        while Instant::now() < deadline {
            if let Ok((topic, payload)) = events_rx.recv_timeout(Duration::from_millis(100)) {
                if topic == "audio/chunk" {
                    let pcm = BASE64.decode(payload["data"].as_str().unwrap()).unwrap();
                    bytes += pcm.len();
                    peak_rms = peak_rms.max(Vad::rms(&pcm));
                } else if topic == "audio/diagnostic" || topic == "audio/status" || topic == "audio/error" || topic == "protocol/ack" {
                    if topic == "audio/status" && payload["playbackVolume"] == 100 { volume_confirmed = true; }
                    diagnostics::record(&id, "device", json!({"stage":payload["event"],"bytes":payload["bytes"],"ok":payload["ok"]}));
                    println!("capture topic={topic} event={} active={} enabled={} conversation={} code={}", payload["event"], payload["active"], payload["enabled"], payload["conversation"], payload["code"]);
                }
            }
            let _ = usb.send_to_board(&board, "system/heartbeat", &json!({}));
        }
        session.cancelled.store(true, Ordering::SeqCst);
        assert!(volume_confirmed, "device codec output volume is not confirmed at 100");
        assert!(bytes > 32_000, "device microphone did not provide sustained PCM: bytes={bytes}");
        diagnostics::record(&id, "hardware_passed", json!({"bytes":bytes,"ok":true}));
        println!("PASS actual cloud TTS device playback completed with {duplex_frames} duplex capture frames and zero echo-triggered starts; microphone bytes={bytes}, peak RMS={peak_rms:.1}");
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "exclusive P4 port and internal cloud defaults; repeats actual duplex device playback three times"]
    fn live_duplex_three_hardware_turns() {
        for turn in 1..=3 {
            live_device_audio();
            println!("PASS actual duplex device round={turn}");
        }
    }

    #[test]
    #[ignore = "exclusive physical P4; metadata-only DSP throughput test with silent duplex playback"]
    fn live_device_dsp_timing() {
        let usb=UsbSerialManager::new();
        let (tx,rx)=std_mpsc::channel();
        usb.connect(&std::env::var("P4_SERIAL_PORT").unwrap(),move |topic,payload| {
            if topic=="audio/chunk" {
                let _=tx.send((topic,json!({"aec":payload["aec"],"seq":payload["seq"]})));
            } else if matches!(topic.as_str(),"audio/status"|"audio/diagnostic") { let _=tx.send((topic,payload)); }
        }).unwrap();
        let board=usb.status().board_device_id;
        assert_eq!(board,std::env::var("P4_EXPECTED_BOARD_ID").unwrap());
        struct Cleanup(UsbSerialManager,String);
        impl Drop for Cleanup {fn drop(&mut self) {
            let _=self.0.send_to_board(&self.1,"audio/conversation",&json!({"enabled":false}));
            self.0.disconnect();
        }}
        let _cleanup=Cleanup(usb.clone(),board.clone());
        let id=format!("dsp-probe-{}",now_ms());
        usb.send_to_board(&board,"audio/conversation",&json!({"enabled":true,"halfDuplex":false,"sessionId":id})).unwrap();
        let duration=std::env::var("P4_DSP_TEST_SECONDS").ok().and_then(|s|s.parse::<u64>().ok()).unwrap_or(12).clamp(5,180);
        let started=Instant::now();let mut frames=0;let mut failures=0;let mut ready=false;
        let mut last_query=Instant::now();let mut next_play=Instant::now();let mut sequence=0;
        let playback=std::env::var("P4_DSP_PLAYBACK").as_deref()==Ok("1");
        let play_id=format!("{id}-play");let silence=BASE64.encode(vec![0u8;3200]);
        while started.elapsed()<Duration::from_secs(duration) {
            if let Ok((topic,payload))=rx.recv_timeout(Duration::from_millis(10)) {
                if topic=="audio/chunk" {frames+=1;if payload["aec"]!=true {failures+=1;}}
                else if payload["conversationSessionId"]==id {
                    let available=payload["audioAec"]==true && payload["audioAecPreparing"]!=true;
                    if topic=="audio/status" && available && !ready {
                        ready=true;
                        if playback {usb.send_to_board(&board,"audio/play_begin",&json!({"sessionId":play_id})).unwrap();}
                    }
                    if payload["event"]=="aec_fallback" {failures+=1;}
                    let mut details=json!({"event":payload["event"],"available":available,"reason":payload["aecReason"]});
                    for key in ["dspFrames","dspLastUs","dspMaxUs","dspAecUs","dspNsUs","dspVadUs","aecReferenceSamples","internalFreeBytes","internalLargestBytes","captureTxLastUs","captureTxMaxUs","captureStackMinBytes"] {details[key]=payload[key].clone();}
                    eprintln!("DSP {}",details);
                }
            }
            if ready && playback && Instant::now()>=next_play {
                usb.send_to_board(&board,"audio/play_chunk",&json!({"sessionId":play_id,"seq":sequence,"bytes":3200,"data":silence})).unwrap();
                sequence+=1;next_play+=Duration::from_millis(100);
                if next_play+Duration::from_millis(500)<Instant::now() {next_play=Instant::now();}
            }
            if last_query.elapsed()>=Duration::from_secs(1) {
                usb.send_to_board(&board,"audio/query",&json!({})).unwrap();
                usb.send_to_board(&board,"system/heartbeat",&json!({})).unwrap();
                last_query=Instant::now();
            }
            if failures>0 {break;}
        }
        eprintln!("DSP SUMMARY frames={frames} elapsedMs={} failures={failures} playback={playback}",started.elapsed().as_millis());
        assert_eq!(failures,0,"device DSP failed");
        assert!(frames>duration*40,"capture did not keep up with 20ms cadence");
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "exclusive P4 and saved cloud credentials; actual TTS, microphone and production ASR-confirmed interruption path"]
    fn live_device_verified_duplex() {
        let dir=std::path::PathBuf::from(std::env::var("PET_REALTIME_SETTINGS_DIR").unwrap());
        voice_chat_settings::configure_storage_dir(dir.clone()).unwrap();
        crate::llm_network::configure_storage_dir(dir.clone()).unwrap();
        crate::volcengine_asr::configure_storage_dir(dir.clone()).unwrap();
        diagnostics::configure(&dir).unwrap();
        let board=std::env::var("P4_EXPECTED_BOARD_ID").unwrap();
        let id=format!("rtc-verified-hil-{}",now_ms());
        let (input,mut rx)=mpsc::channel(128);
        let session=Arc::new(Session{id:id.clone(),board_device_id:board.clone(),cancelled:AtomicBool::new(false),
            duplex:AtomicBool::new(true),capture_armed:AtomicBool::new(false),capture_ready:AtomicBool::new(false),
            stop_notify:tokio::sync::Notify::new(),input,status:Mutex::new(Default::default()),
            completed_playback:Mutex::new(String::new()),pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits())});
        let frames=Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let faults=Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let usb=UsbSerialManager::new();
        let routed=session.clone();let counted=frames.clone();let failed=faults.clone();
        usb.connect(&std::env::var("P4_SERIAL_PORT").unwrap(),move |topic,p| {
            if topic=="audio/chunk" {counted.fetch_add(1,Ordering::Relaxed);}
            if topic=="audio/diagnostic" {
                if p["event"]=="aec_fallback" {failed.fetch_add(1,Ordering::Relaxed);}
                if p["event"]=="playback_completed" {
                    *routed.completed_playback.lock().unwrap()=p["sessionId"].as_str().unwrap_or("").into();
                }
            }
            route_session_audio(&routed,&topic,&p);
        }).unwrap();
        assert_eq!(usb.status().board_device_id,board);
        struct Cleanup(UsbSerialManager,Arc<Session>);
        impl Drop for Cleanup {fn drop(&mut self) {
            self.1.cancelled.store(true,Ordering::SeqCst);
            let _=self.0.send_to_board(&self.1.board_device_id,"audio/play_flush",&json!({}));
            let _=self.0.send_to_board(&self.1.board_device_id,"audio/conversation",&json!({"enabled":false}));
            let _=self.0.send_to_board(&self.1.board_device_id,"ui/conversation",&json!({"state":"ended"}));
            self.0.disconnect();
        }}
        let _cleanup=Cleanup(usb.clone(),session.clone());
        let player=spawn_player(usb.clone(),board,session.clone());
        tauri::async_runtime::block_on(async {
            hud(&usb,&session.board_device_id,&id,"preparing","音频测试","");
            begin_capture(&usb,&session,true).await.unwrap();
            let mut tts=DoubaoTtsClient::new(voice_chat_settings::tts_config("zh_male_naiqimengwa_mars_bigtts","",1.05).unwrap());
            let mut vad=Vad::new();vad.session=Some(session.clone());vad.diagnostic_session=id.clone();
            let mut deferred=VecDeque::new();
            let text="你好，现在正在检查双向实时对话。设备会继续播放，同时保持收音。测试结束后就可以正常聊天了。";
            for turn in 1..=3 {
                {let mut status=session.status.lock().unwrap();status.state="speaking".into();status.reply=text.into();}
                hud(&usb,&session.board_device_id,&id,"speaking","音频测试","");
                let reply=async {
                    player.send(PlayerCmd::Begin(format!("{id}-{turn}"))).unwrap();
                    speak_one(&mut tts,&player,text).await?;
                    wait_playback(&player).await;
                    Ok(())
                };
                let interruption=timeout(Duration::from_secs(40),listen_with_verified_barge_in(reply,&mut rx,&mut deferred,&mut vad,true)).await.unwrap().unwrap();
                if let Some(speech)=interruption.as_ref() {
                    let normalized=|s:&str|s.chars().filter(|c|c.is_alphanumeric()).collect::<String>();
                    let candidate=normalized(&speech.text);
                    eprintln!("VERIFIED candidate chars={} containedInSyntheticReply={}",candidate.chars().count(),normalized(text).contains(&candidate));
                }
                assert!(interruption.is_none(),"actual ASR confirmation interrupted test playback");
                assert!(!session.cancelled.load(Ordering::SeqCst),"capture or playback stopped");
                eprintln!("VERIFIED DUPLEX round={turn} frames={} aecFaults={} confirmedInterruptions=0",frames.load(Ordering::Relaxed),faults.load(Ordering::Relaxed));
            }
            tts.reset().await;
        });
        assert_eq!(faults.load(Ordering::Relaxed),0);
        assert!(frames.load(Ordering::Relaxed)>600);
    }

    fn frame(amplitude: i16) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_BYTES);
        for i in 0..(FRAME_BYTES / 2) {
            let v = if i % 2 == 0 { amplitude } else { -amplitude };
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    #[test]
    #[ignore = "exclusive P4 port; plays supplied test WAV and measures processed microphone metadata only"]
    fn live_device_duplex_echo_probe() {
        let wav=std::fs::read(std::env::var("P4_TEST_WAV").unwrap()).unwrap();
        assert_eq!(&wav[..4], b"RIFF"); assert_eq!(&wav[8..12], b"WAVE");
        let mut at=12; let mut pcm=None; let mut valid=false;
        while at+8<=wav.len() {
            let n=u32::from_le_bytes(wav[at+4..at+8].try_into().unwrap()) as usize;
            assert!(at+8+n<=wav.len()); let chunk=&wav[at+8..at+8+n];
            if &wav[at..at+4]==b"fmt " {
                valid=chunk.len()>=16 && chunk[..4]==[1,0,1,0] && chunk[4..8]==16000u32.to_le_bytes() && chunk[14..16]==[16,0];
            }
            if &wav[at..at+4]==b"data" {pcm=Some(chunk.to_vec());}
            at+=8+n+(n%2);
        }
        assert!(valid); let pcm=pcm.unwrap(); assert!(pcm.len()<=32_000*40 && pcm.len()>32_000);
        let repeats=std::env::var("P4_ECHO_REPEAT").ok().map(|v|v.parse::<usize>().unwrap()).unwrap_or(1);
        assert!((1..=2).contains(&repeats));
        let pcm=pcm.repeat(repeats); assert!(pcm.len()<=32_000*40);
        let usb=UsbSerialManager::new(); let (tx,events)=std_mpsc::channel();
        usb.connect(&std::env::var("P4_SERIAL_PORT").unwrap(),move |topic,payload|{let _=tx.send((topic,payload));}).unwrap();
        let board=usb.status().board_device_id;
        assert_eq!(board,std::env::var("P4_EXPECTED_BOARD_ID").unwrap());
        let caps=usb.status().capabilities;
        println!("duplex capabilities aec={} vad={} duplex={}",caps["features"]["audioAec"],caps["features"]["audioVad"],caps["features"]["audioFullDuplex"]);
        assert!(["audioAec","audioVad","audioFullDuplex"].iter().all(|k|caps["features"][*k]==true));
        struct Cleanup(UsbSerialManager,String);
        impl Drop for Cleanup {fn drop(&mut self){let _=self.0.send_to_board(&self.1,"audio/play_flush",&json!({})); let _=self.0.send_to_board(&self.1,"audio/conversation",&json!({"enabled":false}));self.0.disconnect();}}
        let _cleanup=Cleanup(usb.clone(),board.clone());
        let id=format!("rtc-echo-{}",now_ms()); let (input,_)=mpsc::channel(128);
        let session=Arc::new(Session{id:id.clone(),board_device_id:board.clone(),cancelled:AtomicBool::new(false),duplex:AtomicBool::new(true),capture_armed:AtomicBool::new(true),capture_ready:AtomicBool::new(false),stop_notify:tokio::sync::Notify::new(),input,status:Mutex::new(Default::default()),completed_playback:Mutex::new(String::new()),pending_persona:Mutex::new(None), target_agent_id:String::new(), target_session_id:String::new(), target_unconfirmed:false, playback_volume:AtomicU32::new(1.0f32.to_bits())});
        let player=spawn_player(usb.clone(),board.clone(),session.clone());
        let raw_calibration=std::env::var("P4_ECHO_RAW").as_deref()==Ok("1");
        if raw_calibration {
            usb.send_to_board(&board,"audio/control",&json!({"action":"start"})).unwrap();
        } else {
            usb.send_to_board(&board,"audio/conversation",&json!({"enabled":true,"halfDuplex":false,"sessionId":id})).unwrap();
        }
        let began=Instant::now(); let mut started=false; let mut playing=false; let mut completed=false;
        let mut frames=[0usize;3]; let mut speech=[0usize;3]; let mut energy=[0f64;3];
        let mut max_run=0; let mut run=0; let mut faults=0;
        let mut production_vad=Vad::new(); let mut false_starts=0;
        let barge_test=std::env::var("P4_ECHO_BARGE").as_deref()==Ok("1");
        let mut barge_at: Option<Instant>=None;
        let mut flush_latency=None;
        let mut prompted=false;
        let mut credible_run=0; let mut max_credible_run=0;
        let mut voiced_levels: [Vec<f32>;3] = Default::default();
        let mut raw_mic=Vec::<i16>::new(); let mut playback_start_sample=0usize;
        let capture_only=std::env::var("P4_ECHO_CAPTURE_ONLY").as_deref()==Ok("1");
        let duration=Duration::from_millis(pcm.len() as u64*1000/32000);
        let deadline=Instant::now()+if capture_only {Duration::from_secs(5)} else {duration+Duration::from_secs(10)};
        while Instant::now()<deadline {
            if barge_test && !prompted && began.elapsed()>Duration::from_secs(7) {
                hud(&usb,&board,&id,"speaking","打断测试","现在请说：停一下，我有一个新问题。");
                println!("barge prompt: speak now while device continues playing"); prompted=true;
            }
            if flush_latency.is_some() && barge_at.map(|at|at.elapsed()>Duration::from_secs(3)).unwrap_or(false) {break;}
            if !capture_only && !started && began.elapsed()>Duration::from_secs(3) {
                println!("echo baseline frames={} (expected about 150 in 3 s)", frames[0]);
                if frames[0]<110 || faults>0 {break;}
                player.send(PlayerCmd::Begin(format!("{id}-play"))).unwrap();
                player.send(PlayerCmd::Pcm(pcm.clone())).unwrap();
                let (done,_)=std_mpsc::channel();player.send(PlayerCmd::End(done)).unwrap();started=true;
            }
            if let Ok((topic,p))=events.recv_timeout(Duration::from_millis(30)) {
                if topic=="audio/diagnostic" {
                    if p["event"]=="playback_started" {playing=true;playback_start_sample=raw_mic.len();}
                    if p["event"]=="playback_completed" {
                        playing=false;completed=true;
                        production_vad.reply_tail_until=Some(Instant::now()+POST_SPEAK_GUARD);
                    }
                    if p["event"]=="playback_flushed" {
                        if let Some(at)=barge_at {
                            if flush_latency.is_none() {flush_latency=Some(at.elapsed());playing=false;}
                        }
                    }
                    if p["event"]=="aec_fallback" || p["event"]=="playback_overflow" {faults+=1;}
                    if p["ok"]==false {println!("device diagnostic event={}",p["event"]);}
                }
                if topic=="audio/error" {faults+=1;println!("audio error code={}",p["code"]);}
                if topic=="audio/chunk" {
                    if !raw_calibration && p["aec"]!=true {faults+=1;continue;}
                    let data=BASE64.decode(p["data"].as_str().unwrap()).unwrap();
                    if raw_calibration {raw_mic.extend(data.chunks_exact(2).map(|b|i16::from_le_bytes([b[0],b[1]])));}
                    let phase=if playing{1}else if completed{2}else{0};
                    frames[phase]+=1;energy[phase]+=Vad::rms(&data) as f64;
                    if playing && p["vadSpeech"]==true && Vad::rms(&data)>=PROCESSED_VAD_ABS_MIN {
                        credible_run+=1;max_credible_run=max_credible_run.max(credible_run);
                    } else {credible_run=0;}
                    let context = if started && !completed && flush_latency.is_none() { VadContext::Reply } else { VadContext::Listening };
                    if let VadStep::Start(_) = production_vad.step_in_context(&data,p["vadSpeech"].as_bool(),context) {
                        if barge_test && prompted && barge_at.is_none() {
                            barge_at=Some(Instant::now());
                            player.send(PlayerCmd::Flush).unwrap();
                        } else if !barge_test || !prompted {false_starts+=1;}
                        println!("echo vad_start phase={phase} at_ms={} rms={:.1}",began.elapsed().as_millis(),Vad::rms(&data));
                    }
                    if p["vadSpeech"]==true {voiced_levels[phase].push(Vad::rms(&data));}
                    if p["vadSpeech"]==true {speech[phase]+=1;if playing {run+=1;max_run=max_run.max(run);}} else {run=0;}
                }
            }
        }
        session.cancelled.store(true,Ordering::SeqCst);
        if barge_test {hud(&usb,&board,&id,"ended","","");}
        for i in 0..3 {
            voiced_levels[i].sort_by(|a,b|a.total_cmp(b));
            let levels=&voiced_levels[i];
            println!("echo phase={i} frames={} voiced={} mean_rms={:.1} voiced_p95={:.1} voiced_max={:.1}",frames[i],speech[i],energy[i]/frames[i].max(1) as f64,
                levels.get(levels.len()*95/100).copied().unwrap_or(0.0),levels.last().copied().unwrap_or(0.0));
        }
        println!("echo production_vad_starts={false_starts} max_credible_ms={}",max_credible_run*20);
        println!("echo longest_voiced_ms={} playback_completed={completed} faults={faults}",max_run*20);
        if barge_test {
            assert_eq!(false_starts,0,"speaker echo interrupted before the speak prompt");
            let latency=flush_latency.expect("no near-end speech / device flush acknowledgement");
            println!("barge VAD-to-device-flush={} ms (codec DMA tail up to 30 ms; not mouth-to-stop latency)",latency.as_millis());
            assert!(latency<Duration::from_millis(300) && faults==0,"barge-in transport latency/fault");
            return;
        }
        if raw_calibration {
            let reference:Vec<f64>=pcm.chunks_exact(2).map(|b|i16::from_le_bytes([b[0],b[1]]) as f64).collect();
            let mut best=(0isize,0f64);
            for lag in (-4096isize..8192).step_by(8) {
                let mut dot=0f64;let mut x2=0f64;let mut y2=0f64;
                for i in (16000..reference.len().min(64000)).step_by(8) {
                    let j=playback_start_sample as isize+i as isize+lag;
                    if j<0 || j as usize>=raw_mic.len(){continue;}
                    let x=reference[i];let y=raw_mic[j as usize] as f64;
                    dot+=x*y;x2+=x*x;y2+=y*y;
                }
                let score=dot.abs()/(x2*y2).sqrt().max(1.0);
                if score>best.1 {best=(lag,score);}
            }
            println!("acoustic alignment mic_lag_samples={} mic_lag_ms={:.2} correlation={:.3} clipped={}/{}",best.0,best.0 as f64/16.0,best.1,raw_mic.iter().filter(|s|s.unsigned_abs()>32000).count(),raw_mic.len());
            assert!(completed && frames[1]>50 && faults==0,"raw calibration stream failed");
            return;
        }
        if capture_only {
            assert!(frames[0]>=200 && faults==0,"capture alone cannot keep real-time cadence");
            return;
        }
        assert!(completed && frames[1]>50 && faults==0,"full-duplex stream/codec failed");
        assert_eq!(false_starts,0,"speaker-only playback would trigger the production VAD; needs acoustic tuning");
    }

    #[test]
    fn vad_starts_on_sustained_speech_and_ends_after_silence() {
        let mut vad = Vad::new();
        for _ in 0..20 {
            assert!(matches!(vad.step(&frame(50)), VadStep::Idle));
        }
        let mut started = false;
        for _ in 0..SPEECH_START_FRAMES {
            if let VadStep::Start(preroll) = vad.step(&frame(6000)) {
                started = true;
                assert!(!preroll.is_empty());
            }
        }
        assert!(started);
        for _ in 0..5 {
            assert!(matches!(vad.step(&frame(6000)), VadStep::Speech));
        }
        let mut ended = false;
        for _ in 0..SPEECH_END_FRAMES {
            if let VadStep::End = vad.step(&frame(40)) {
                ended = true;
            }
        }
        assert!(ended);
    }

    #[test]
    fn realtime_key_events_are_recognized_by_action_id() {
        let payload = json!({ "action": "realtime_chat", "context": "main", "boardDeviceId": "b1" });
        assert_eq!(payload.get("action").and_then(Value::as_str), Some(ACTION_ID));
        for page in ["main", "app", "components"] { assert!(realtime_entry_context(page)); }
        for page in ["", "unknown", "settings"] { assert!(!realtime_entry_context(page)); }
    }
}
