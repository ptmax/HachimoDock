/**
 * [Input] Consume app composition defined by `pc/src/App.jsx`[Pos] and shared styling defined by `pc/src/styles.css`[Pos].
 * [Output] Provide React bootstrap entry to downstream Vite runtime mounting.
 * [Pos] entry node in pc/src
 * [Sync] If this file changes, update this header and `pc/src/.folder.md`.
 */

import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.jsx";
import "./styles.css";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { startBubblePixelService } from "./lib/bubble-pixels.js";
import {
  TEXT_RENDERING_CHANGE_EVENT,
  textRenderingModeForTransport,
} from "./lib/text-rendering-settings.js";

if (isTauri() && getCurrentWindow().label === "main") {
  let stopHostTextRendering = null;
  let syncing = false;
  const syncHostTextRendering = async () => {
    if (syncing) return;
    syncing = true;
    try {
      const useApp = textRenderingModeForTransport("usb") === "app";
      if (useApp && !stopHostTextRendering) {
        stopHostTextRendering = await startBubblePixelService({ listen, invoke });
      } else if (!useApp && stopHostTextRendering) {
        const stop = stopHostTextRendering;
        stopHostTextRendering = null;
        stop();
      }
    } catch (error) {
      stopHostTextRendering = null;
      console.warn("Device text rendering unavailable; retaining client rendering.", error);
    } finally {
      syncing = false;
    }
  };
  void syncHostTextRendering();
  window.addEventListener(TEXT_RENDERING_CHANGE_EVENT, syncHostTextRendering);
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
