/**
 * [Input] DeepSeek Harness production wiring in Rust discovery/allowlists and the managed bridge sidecar.
 * [Output] Static regression coverage that DSH registers with the agent-session-bus and stays out of visible-composer paths.
 * [Pos] test node in pc/src
 */

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const read = (relativePath) => readFileSync(new URL(relativePath, import.meta.url), "utf8");

test("managed bridge sidecar registers the DeepSeek Harness adapter", () => {
  const bridge = read("../src-tauri/bridge/packages/clawd-backend-service/src/headless-mqtt.js");
  const busIndex = read("../src-tauri/bridge/packages/agent-session-bus/src/index.js");
  const busPackage = JSON.parse(read("../src-tauri/bridge/packages/agent-session-bus/package.json"));

  assert.match(busIndex, /DshAdapter/);
  assert.match(bridge, /^\s+DshAdapter,\s*$/m);
  assert.match(bridge, /new DshAdapter\(\{ log: busLog \}\)/);
  assert.equal(busPackage.exports["./adapters/dsh"], "./src/adapters/dsh.js");
});

test("Rust keeps DeepSeek Harness on the bus-injection path", () => {
  const rust = read("../src-tauri/src/lib.rs");

  assert.match(rust, /detect_dsh\(\),/);
  assert.match(rust, /KNOWN_USB_STATE_SOURCES: \[&str; 6\]/);
  assert.match(
    rust,
    /"deepseek-harness" \| "deepseek" \| "dsh" => Some\("deepseek-harness"\.to_string\(\)\)/,
  );
  assert.match(rust, /"deepseek-harness" => "deepseek-harness"\.to_string\(\)/);
  assert.match(
    rust,
    /"deepseek-harness" \| "deepseek" \| "dsh" => "DeepSeek Harness"\.to_string\(\)/,
  );
  // DSH is injected through agent-session-bus, so it must never join the AX
  // visible-composer set (which would write into a desktop editor instead).
  assert.doesNotMatch(
    rust,
    /fn agent_uses_visible_composer\(agent_id: &str\) -> bool \{\s*matches!\([^)]*deepseek-harness/,
  );
});

test("DSH interrupt action is wired end to end and gated by selected agent", () => {
  const rust = read("../src-tauri/src/lib.rs");
  const dashboard = read("DeviceDashboard.jsx");
  const panel = read("dashboard/BoardButtonPanel.jsx");
  assert.match(rust, /dsh_interrupt/);
  assert.match(rust, /fn interrupt_dsh_session/);
  assert.match(dashboard, /dsh_interrupt/);
  assert.match(panel, /selectedAgentId !== "deepseek-harness"/);
});

test("realtime DSH task dispatch uses the plugin task API and never the visible composer", () => {
  const desktop = read("../src-tauri/src/desktop_agent.rs");
  const dshTask = read("../src-tauri/src/dsh_task.rs");
  const realtime = read("../src-tauri/src/realtime_chat.rs");
  const approval = read("../src-tauri/src/dsh_approval.rs");
  const plugin = read("../../integrations/dsh-hachimodock/dsh-bridge.js");
  const pluginServer = read("../../integrations/dsh-hachimodock/server.js");
  const pluginTasks = read("../../integrations/dsh-hachimodock/tasks.js");

  // The DSH adapter dispatches through `/task`, never the desktop composer.
  assert.match(desktop, /Agent::Dsh/);
  assert.match(desktop, /fn dsh_follow_block/);
  assert.match(desktop, /fn run_dsh_dispatch/);
  assert.match(desktop, /fn monitor_dsh/);
  assert.match(desktop, /crate::dsh_task::dispatch/);
  assert.match(desktop, /cancel_running/);
  assert.doesNotMatch(desktop, /Agent::Dsh[\s\S]{0,400}?dispatch_task_from_draft/);

  // Transport is loopback-only and keeps fixed codes out of the conversation.
  assert.match(dshTask, /HACHIMODOCK_DSH_URL/);
  assert.match(dshTask, /POST \/task|\/task/);
  assert.match(dshTask, /DSH_BRIDGE_UNAVAILABLE/);
  assert.match(dshTask, /no_proxy/);
  assert.match(dshTask, /takeover_once/);
  assert.match(dshTask, /DSH_TEXT_TOO_LARGE/);
  assert.match(dshTask, /is_connect/);
  assert.match(desktop, /outcome\.adopted/);
  assert.match(desktop, /DispatchError::Uncertain/);
  assert.match(desktop, /take_permission_notification/);
  assert.match(realtime, /take_permission_notification/);

  // Approval is a hard gate for realtime utterance dispatch.
  assert.match(approval, /pub fn device_approval_pending/);
  assert.match(realtime, /device_approval_pending/);
  assert.match(realtime, /approval_gated/);
  assert.match(realtime, /handle_dsh_interrupt_action/);

  // Plugin contract and bounded task state machine.
  assert.match(pluginServer, /path === '\/task'/);
  assert.match(plugin, /agent\/inbox\/claimed/);
  assert.match(plugin, /dispatchTask/);
  assert.match(pluginTasks, /terminalStateForReason/);
  assert.match(pluginTasks, /not found|ACTIVE_STATES/);
});
