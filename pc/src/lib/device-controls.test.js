import test from "node:test";
import assert from "node:assert/strict";
import { normalizeDeviceControls } from "./device-controls.js";

test("device controls clamp unsafe volume and brightness", () => {
  assert.deepEqual(normalizeDeviceControls({ volume: 200, brightness: 0 }), {
    volume: 90,
    brightness: 10,
  });
  assert.deepEqual(normalizeDeviceControls({}), { volume: 70, brightness: 80 });
});
