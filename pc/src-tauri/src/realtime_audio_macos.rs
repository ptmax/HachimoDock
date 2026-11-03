//! App Nap protection for background USB work and for the dedicated audio player.
//! The audio activity is created on the player thread, never on the UI or a
//! shared async worker; the process activity is created once in `run()` and
//! lives for the app lifetime.
use objc2::{rc::Retained, runtime::ProtocolObject};
use objc2_foundation::{NSActivityOptions, NSObjectProtocol, NSProcessInfo, NSString};
use std::{marker::PhantomData, rc::Rc};

pub(crate) struct AudioActivity {
    activity: ProcessActivity,
    pub(crate) qos_error: i32,
    // The QoS setting belongs to this dedicated thread, which exits with the
    // player. Prevent moving this guard to another thread or an async task.
    _thread_bound: PhantomData<Rc<()>>,
}

impl AudioActivity {
    pub(crate) fn begin_on_player_thread() -> Self {
        // A 250 us sleep per 64-byte CH343 slice can become ~6 ms at background
        // QoS. ~69 such sleeps then starve each 100 ms PCM packet. Explicit QoS
        // and an activity assertion address both scheduling and App Nap; keep
        // the hardware-safe pacing and avoid a continuous busy-wait loop.
        let qos_error = unsafe {
            libc::pthread_set_qos_class_self_np(
                libc::qos_class_t::QOS_CLASS_USER_INITIATED, 0,
            )
        };
        Self {
            activity: ProcessActivity::begin("Realtime conversation USB audio"),
            qos_error,
            _thread_bound: PhantomData,
        }
    }
}

/// Process-lifetime activity for background USB work (auto-connect, heartbeat
/// liveness probes, port recovery). It uses the same options as the audio
/// activity: user-initiated scheduling plus latency-critical handling while
/// idle/lid system sleep stays allowed. It never sets PreventSystemSleep, so
/// the keep-awake switch keeps its meaning.
pub(crate) struct ProcessActivity {
    process: Retained<NSProcessInfo>,
    token: Retained<ProtocolObject<dyn NSObjectProtocol>>,
}

impl ProcessActivity {
    pub(crate) fn begin(reason: &str) -> Self {
        let (process, token) = begin_activity(reason);
        Self { process, token }
    }
}

impl Drop for ProcessActivity {
    fn drop(&mut self) {
        // SAFETY: token was returned by beginActivity on this process and has
        // exactly one owner/end call. Retained releases it after endActivity.
        unsafe { self.process.endActivity(&self.token); }
    }
}

fn begin_activity(
    reason: &str,
) -> (
    Retained<NSProcessInfo>,
    Retained<ProtocolObject<dyn NSObjectProtocol>>,
) {
    let process = NSProcessInfo::processInfo();
    let token = process.beginActivityWithOptions_reason(
        activity_options(),
        &NSString::from_str(reason),
    );
    (process, token)
}

fn activity_options() -> NSActivityOptions {
    // Do not keep the screen awake or block user/system sleep. The audio guard
    // ends on player channel close/cancellation; the process guard ends with the
    // app. Neither requests PreventSystemSleep.
    NSActivityOptions::UserInitiatedAllowingIdleSystemSleep | NSActivityOptions::LatencyCritical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_preserves_display_and_system_sleep() {
        let options = activity_options();
        assert!(options.contains(NSActivityOptions::LatencyCritical));
        assert!(!options.intersects(NSActivityOptions::IdleDisplaySleepDisabled | NSActivityOptions::IdleSystemSleepDisabled));
    }

    #[test]
    fn process_activity_never_prevents_system_sleep() {
        let options = activity_options();
        assert!(!options.intersects(
            NSActivityOptions::IdleSystemSleepDisabled
                | NSActivityOptions::IdleDisplaySleepDisabled
        ));
        // Creating and dropping the guard must work off the main thread too:
        // it is held for the whole app lifetime, not tied to UI or audio work.
        std::thread::spawn(|| {
            let activity = ProcessActivity::begin("HachimoDock background USB test");
            drop(activity);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn player_thread_promotes_inherited_background_qos() {
        std::thread::spawn(|| {
            assert_eq!(unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_BACKGROUND, 0) }, 0);
            let activity = AudioActivity::begin_on_player_thread();
            assert_eq!(activity.qos_error, 0);
            let mut qos = libc::qos_class_t::QOS_CLASS_UNSPECIFIED;
            let mut priority = 0;
            assert_eq!(unsafe { libc::pthread_get_qos_class_np(libc::pthread_self(), &mut qos, &mut priority) }, 0);
            assert_eq!(qos as u32, libc::qos_class_t::QOS_CLASS_USER_INITIATED as u32);
            drop(activity);
            // No shared worker priority is changed: this dedicated thread exits.
        }).join().unwrap();
    }
}
