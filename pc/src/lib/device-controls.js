/**
 * [Input] Browser localStorage for the P4 output volume and backlight.
 * [Output] Bounded device-control values safe to send over USB.
 * [Pos] shared device settings helper in pc/src/lib
 */

export const DEVICE_CONTROLS_STORAGE_KEY = "hachimodock.device-controls.v1";
export const DEVICE_VOLUME_MIN = 0;
export const DEVICE_VOLUME_MAX = 90;
export const DEVICE_BRIGHTNESS_MIN = 10;
export const DEVICE_BRIGHTNESS_MAX = 100;
export const DEFAULT_DEVICE_CONTROLS = Object.freeze({ volume: 70, brightness: 80 });

const clamp = (value, min, max) => Math.min(max, Math.max(min, Math.round(Number(value))));

export function normalizeDeviceControls(value = {}) {
  const volume = Number.isFinite(Number(value.volume))
    ? clamp(value.volume, DEVICE_VOLUME_MIN, DEVICE_VOLUME_MAX)
    : DEFAULT_DEVICE_CONTROLS.volume;
  const brightness = Number.isFinite(Number(value.brightness))
    ? clamp(value.brightness, DEVICE_BRIGHTNESS_MIN, DEVICE_BRIGHTNESS_MAX)
    : DEFAULT_DEVICE_CONTROLS.brightness;
  return { volume, brightness };
}

export function loadDeviceControls() {
  try {
    return normalizeDeviceControls(JSON.parse(
      window.localStorage.getItem(DEVICE_CONTROLS_STORAGE_KEY) || "{}",
    ));
  } catch {
    return { ...DEFAULT_DEVICE_CONTROLS };
  }
}

export function saveDeviceControls(value) {
  const next = normalizeDeviceControls(value);
  window.localStorage.setItem(DEVICE_CONTROLS_STORAGE_KEY, JSON.stringify(next));
  return next;
}
