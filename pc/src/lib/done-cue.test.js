import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

function readWav(name) {
  return readFileSync(new URL(`../../public/terrier-clips/${name}.wav`, import.meta.url));
}

function assertPcm16Mono16k(wav, label, expectedDurationMs) {
  assert.equal(wav.toString("ascii", 0, 4), "RIFF", `${label} RIFF`);
  assert.equal(wav.toString("ascii", 8, 12), "WAVE", `${label} WAVE`);
  assert.equal(wav.readUInt16LE(20), 1, `${label} PCM format`);
  assert.equal(wav.readUInt16LE(22), 1, `${label} mono`);
  assert.equal(wav.readUInt32LE(24), 16000, `${label} sample rate`);
  assert.equal(wav.readUInt16LE(34), 16, `${label} bits per sample`);
  const dataBytes = wav.readUInt32LE(40);
  assert.equal(dataBytes, wav.length - 44, `${label} data size`);
  assert.equal(dataBytes / 32000, expectedDurationMs / 1000, `${label} duration`);
}

test("session-ended cue is the soft 3.8s P4 system cue", () => {
  const wav = readWav("done");
  assertPcm16Mono16k(wav, "done", 3800);
  const factory = JSON.parse(readFileSync(new URL("../../../firmware/factory-config.json", import.meta.url)));
  const manifest = JSON.parse(readFileSync(new URL(`../../public/terrier-clips/p4-ready/${factory.appearance.readyProfile}/p4/manifest.json`, import.meta.url)));
  assert.equal(manifest.systemCues, true);
  assert.ok(manifest.families.every((family) => !family.audioPath));
});

test("permission cues are bundled with the P4 firmware", () => {
  const expected = {
    permission_request: 440,
    permission_approved: 305,
    permission_denied: 320,
    permission_expired: 190,
  };
  const cmake = readFileSync(new URL("../../../firmware/main/CMakeLists.txt", import.meta.url), "utf8");
  assert.match(cmake, /foreach\(cue done error\)/);
  assert.match(cmake, /foreach\(cue permission_request permission_approved permission_denied permission_expired\)/);
  assert.match(cmake, /pc\/public\/terrier-clips\/\$\{cue\}\.wav/);
  for (const [name, duration] of Object.entries(expected)) {
    assertPcm16Mono16k(readWav(name), name, duration);
  }
});

test("deduplicated waiting cue stays byte-identical to error cue", () => {
  assert.deepEqual(readWav("waiting_user"), readWav("error"));
});
