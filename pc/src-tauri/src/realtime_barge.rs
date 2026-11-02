//! Text evidence for speculative ASR. No transcript or PCM is persisted.
use std::time::Duration;
use std::collections::HashSet;
pub fn protect_previous_tail(candidate_age: Option<Duration>) -> bool {
    candidate_age.is_some_and(|age| age < Duration::from_millis(800))
}

fn bigrams(text: &str) -> HashSet<(char, char)> {
    text.chars().zip(text.chars().skip(1)).collect()
}

/// Playback echo is often transcribed with a slightly different ending or a
/// missing character. Character bigram overlap catches that case while still
/// allowing unrelated short commands.
fn echo_like(text: &str, reply: &str) -> bool {
    let text = normalized(text);
    let reply = normalized(reply);
    if text.chars().count() < 3 || reply.chars().count() < 3 { return false; }
    if reply.contains(&text) { return true; }
    let a = bigrams(&text);
    let b = bigrams(&reply);
    if a.is_empty() || b.is_empty() { return false; }
    let shared = a.intersection(&b).count();
    shared * 100 >= a.len() * 55
}


fn explicit_barge_command(text: &str) -> bool {
    matches!(text, "停" | "停止" | "换歌" | "下一首" | "取消")
}

pub fn normalized(text: &str) -> String {
    text.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect()
}
#[derive(Default)]
pub struct Confirmation { previous: String, first_ms: Option<u128>, revisions: usize }
impl Confirmation {
    pub fn accept(&mut self, text: &str, terminal: bool, confidence: Option<f64>, elapsed: Duration,
        voiced_frames: usize, phase: &str, reply: &str, previous_user: &str) -> bool {
        let text = normalized(text);
        if confidence.is_some_and(|c| !c.is_finite() || c < 0.6) { return false; }
        if text == "停" { return true; }
        if text.chars().count() < 2 { return false; }
        let ms = elapsed.as_millis();
        let previous_user = normalized(previous_user);
        if !previous_user.is_empty() && previous_user.ends_with(&text) { return false; }
        // ASR alone is not a speaker detector: reject recognized playback echo.
        // Two-character residual echo is still echo. The previous >=3 guard
        // let a final "你好" from the speaker satisfy the two-character user
        // gate and cancel playback. Explicit "停" was handled above; unrelated
        // short commands and speech outside playback remain eligible.
        if phase == "speaking" {
            if echo_like(&text, reply) { return false; }
            // Short ASR fragments during playback are frequently noise or echo.
            // Keep explicit transport commands immediate, but require a real
            // phrase before interrupting a spoken reply.
            if !explicit_barge_command(&text) && text.chars().count() < 6 { return false; }
        }
        if self.previous.is_empty() || !text.starts_with(&self.previous) {
            self.first_ms = Some(ms); self.revisions = 1;
        } else if text != self.previous { self.revisions += 1; }
        self.previous = text;
        if terminal { return voiced_frames >= 8; }
        let stable_ms = if phase == "thinking" { 250 } else { 180 };
        voiced_frames >= 12 && self.revisions >= 2 && ms.saturating_sub(self.first_ms.unwrap_or(ms)) >= stable_ms
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cloud_latency_does_not_turn_the_previous_sentence_tail_into_a_new_command() {
        assert!(protect_previous_tail(Some(Duration::from_millis(1))));
        assert!(!protect_previous_tail(Some(Duration::from_millis(800))));
        assert!(!protect_previous_tail(None));
        let mut c = Confirmation::default();
        let previous = "帮我加一条明天下午买火车票的待办";
        for ms in [400, 2073, 3000] {
            assert!(!c.accept("的待办",true,None,Duration::from_millis(ms),20,"thinking","",previous));
            assert!(!c.accept(previous,true,None,Duration::from_millis(ms),20,"thinking","",previous));
        }
        assert!(c.accept("改成后天",true,None,Duration::from_millis(2073),20,"thinking","",previous));
        assert!(c.accept("停",true,None,Duration::from_millis(2073),20,"thinking","",previous));
        // A deliberately repeated command outside the captured tail window is new speech.
        assert!(c.accept("的待办",true,None,Duration::from_millis(2073),20,"thinking","",""));
    }
    #[test]
    fn temporary_words_noise_and_revisions_cannot_cancel_tasks() {
        let mut c=Confirmation::default();
        assert!(!c.accept("你好",false,None,Duration::ZERO,20,"thinking","",""));
        assert!(!c.accept("你好",false,None,Duration::from_millis(500),40,"thinking","",""));
        assert!(!c.accept("谢谢",false,None,Duration::from_millis(600),40,"thinking","",""));
        assert!(c.accept("谢谢你",false,None,Duration::from_millis(900),45,"thinking","",""));
    }
    #[test]
    fn echo_previous_tail_and_low_confidence_are_rejected() {
        let mut c=Confirmation::default();
        assert!(!c.accept("今天小米",true,None,Duration::from_millis(1000),40,"speaking","今天小米涨了",""));
        assert!(!c.accept("查股价",true,None,Duration::from_millis(400),20,"thinking","","查股价"));
        assert!(!c.accept("停",true,Some(0.2),Duration::ZERO,8,"speaking","",""));
        assert!(!c.accept("换歌",true,None,Duration::ZERO,3,"speaking","",""));
    }
    #[test]
    fn final_short_speech_and_single_stop_still_work() {
        let mut c=Confirmation::default();
        assert!(c.accept("停！",false,None,Duration::ZERO,8,"thinking","",""));
        assert!(c.accept("换歌",true,None,Duration::from_millis(900),8,"speaking","股价上涨",""));
        assert!(!c.accept("嗯",true,None,Duration::from_millis(900),30,"thinking","",""));
    }

    #[test]
    fn two_character_playback_fragments_cannot_confirm_a_barge_in() {
        let mut c=Confirmation::default();
        for terminal in [false,true] {
            assert!(!c.accept("你好。",terminal,None,Duration::from_millis(1200),72,"speaking","你好，现在正在检查双向实时对话。",""));
            assert!(!c.accept("对话",terminal,None,Duration::from_millis(1200),72,"speaking","正在检查双向实时对话",""));
        }
        assert!(c.accept("换歌",true,None,Duration::from_millis(900),12,"speaking","你好，现在正在检查双向实时对话",""));
        let mut c=Confirmation::default();
        assert!(!c.accept("检查双向实时",true,None,Duration::from_millis(1200),60,"speaking","现在正在检查双向实时对话",""));
        assert!(!c.accept("测试一下",true,None,Duration::from_millis(1800),120,"speaking","现在正在检查双向实时对话",""));
        assert!(c.accept("请帮我换一首歌",true,None,Duration::from_millis(1800),20,"speaking","现在正在检查双向实时对话",""));
        assert!(c.accept("停",true,None,Duration::from_millis(200),8,"speaking","你可以说停来结束",""));
        assert!(c.accept("你好",true,None,Duration::from_millis(900),12,"thinking","你好",""));
    }
}
