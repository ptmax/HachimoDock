"""Fail-safe capture teardown contract for SW1/PTT and realtime stop.

Hardware 2026-11-03: P4 reset shortly after SW1 voice recognition ended (and in
other post-audio windows). `pet_p4_audio_capture_stop()` only cleared the request
flag; the audio task could still be inside `esp_codec_dev_read`, while the next
caller (approval cue, status cue selection, control reply) resumed speaker
playback. This test compiles the production capture-ownership predicate and the
bounded settle barrier with mocked atomics/delay and locks the guard usage in
the status-cue producer and playback task.
"""
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def test_capture_stop_settles_the_audio_task_and_blocks_playback():
    source = (ROOT / "main/pet_p4_audio.c").read_text()
    functions = (
        "static bool capture_owns_microphone("
        + source.split("static bool capture_owns_microphone(", 1)[1].split(
            "static uint64_t fnv1a64_update(", 1
        )[0]
        + "esp_err_t pet_p4_audio_capture_stop_owned("
        + source.split("esp_err_t pet_p4_audio_capture_stop_owned(", 1)[1].split(
            "bool pet_p4_audio_conversation_active(void)", 1
        )[0]
    )
    harness = r'''
#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

typedef int esp_err_t;
typedef int portMUX_TYPE;
#define ESP_OK 0
#define ESP_ERR_INVALID_ARG 0x102
#define ESP_ERR_INVALID_STATE 0x103
#define PET_P4_AUDIO_CAPTURE_STOP_SETTLE_MS 100
#define portENTER_CRITICAL(lock) do { (void) (lock); } while (0)
#define portEXIT_CRITICAL(lock) do { (void) (lock); } while (0)

static atomic_bool g_capture_requested;
static atomic_bool g_capture_active;
static atomic_bool g_conversation;
static portMUX_TYPE g_conversation_lock;
static char g_capture_owner[64];
static bool g_ready = true;
static int g_delays;
static int g_active_clear_after = -1; /* -1: never clears */

static void vTaskDelay(int ticks) {
  (void) ticks;
  g_delays += 1;
  if (g_active_clear_after > 0) {
    g_active_clear_after -= 1;
    if (g_active_clear_after == 0) atomic_store(&g_capture_active, false);
  }
}

static bool pet_p4_audio_ready(void) { return g_ready; }

FUNCTIONS

static void reset_state(void) {
  atomic_store(&g_capture_requested, false);
  atomic_store(&g_capture_active, false);
  atomic_store(&g_conversation, false);
  g_capture_owner[0] = '\0';
  g_ready = true;
  g_delays = 0;
  g_active_clear_after = -1;
}

int main(void) {
  /* Ownership predicate is the single guard for cue/playback paths. */
  reset_state();
  assert(!capture_owns_microphone());
  atomic_store(&g_capture_requested, true);
  assert(capture_owns_microphone());
  atomic_store(&g_capture_requested, false);
  atomic_store(&g_capture_active, true);
  assert(capture_owns_microphone());

  /* Stop clears the request and waits for the audio task to leave the codec. */
  reset_state();
  atomic_store(&g_capture_requested, true);
  atomic_store(&g_capture_active, true);
  g_active_clear_after = 3;
  assert(pet_p4_audio_capture_stop() == ESP_OK);
  assert(!atomic_load(&g_capture_requested));
  assert(!atomic_load(&g_capture_active));
  assert(g_delays == 3);

  /* The barrier is bounded when the capture task never reports inactive. */
  reset_state();
  atomic_store(&g_capture_requested, true);
  atomic_store(&g_capture_active, true);
  assert(pet_p4_audio_capture_stop() == ESP_OK);
  assert(g_delays == PET_P4_AUDIO_CAPTURE_STOP_SETTLE_MS);

  /* Not-ready devices are rejected before any wait. */
  reset_state();
  g_ready = false;
  atomic_store(&g_capture_active, true);
  assert(pet_p4_audio_capture_stop() == ESP_ERR_INVALID_STATE);
  assert(g_delays == 0);
  assert(atomic_load(&g_capture_active));

  /* Owner-scoped stop waits only when it actually stopped that capture. */
  reset_state();
  strcpy(g_capture_owner, "notes-1");
  atomic_store(&g_capture_requested, true);
  atomic_store(&g_capture_active, true);
  g_active_clear_after = 2;
  assert(pet_p4_audio_capture_stop_owned("notes-1") == ESP_OK);
  assert(!atomic_load(&g_capture_requested));
  assert(g_delays == 2);
  reset_state();
  strcpy(g_capture_owner, "notes-1");
  atomic_store(&g_capture_active, true);
  assert(pet_p4_audio_capture_stop_owned("other") == ESP_ERR_INVALID_STATE);
  assert(g_delays == 0);
  assert(atomic_load(&g_capture_active));
  /* A conversation owns capture: a component owner must not stop or wait. */
  reset_state();
  strcpy(g_capture_owner, "x");
  atomic_store(&g_conversation, true);
  atomic_store(&g_capture_active, true);
  assert(pet_p4_audio_capture_stop_owned("x") == ESP_ERR_INVALID_STATE);
  assert(g_delays == 0);
  return 0;
}
'''.replace("FUNCTIONS", functions)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        (path / "test.c").write_text(harness)
        subprocess.run(
            ["cc", "-std=gnu11", "-Wall", "-Wextra", "-Werror",
             str(path / "test.c"), "-o", str(path / "test")],
            check=True,
        )
        subprocess.run([str(path / "test")], check=True)

    # The status-cue producer and the cue player must both honor capture
    # ownership, and the settle loop must stay bounded.
    process = source.split("void pet_p4_audio_process(", 1)[1].split(
        "esp_err_t pet_p4_audio_play_event_cue(", 1
    )[0]
    assert "capture_owns_microphone()" in process
    playback = source.split("static void playback_task(", 1)[1].split(
        "static void audio_task(", 1
    )[0]
    assert "capture_owns_microphone()" in playback
    settle = source.split("static void wait_capture_quiesced(", 1)[1].split(
        "static uint64_t fnv1a64_update(", 1
    )[0]
    assert "PET_P4_AUDIO_CAPTURE_STOP_SETTLE_MS" in settle
    assert "vTaskDelay(1)" in settle
    assert "for (;;)" not in settle
    assert source.count("wait_capture_quiesced();") >= 2
