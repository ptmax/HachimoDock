/**
 * [Input] Agent Markdown from DSH/Codex/Claude session text.
 * [Output] Plain text suitable for the P4's local MiSans renderer, preserving line breaks and readable list bullets.
 * [Pos] shared device-text normalizer in pc/src/lib
 */

const FENCE = /^\s*(`{3,}|~{3,})/;
const TABLE_SEPARATOR = /^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$/;

export function flattenMarkdownForDevice(value) {
  const source = String(value ?? "").replace(/\r\n?/g, "\n");
  const lines = [];
  let fence = "";
  for (let line of source.split("\n")) {
    const boundary = line.match(FENCE);
    if (boundary) {
      if (!fence) { fence = boundary[1][0]; continue; }
      if (fence === boundary[1][0]) { fence = ""; continue; }
    }
    if (!line.trim()) { lines.push(""); continue; }
    if (fence) { lines.push(line.replace(/\s+$/, "")); continue; }
    if (TABLE_SEPARATOR.test(line)) continue;
    line = line.replace(/^\s{0,3}#{1,6}\s+/, "");
    line = line.replace(/^\s*(?:>\s*)+/, "");
    line = line.replace(/^\s*[-+*]\s+\[([ xX])\]\s+/, (_, checked) => checked === " " ? "☐ " : "☑ ");
    line = line.replace(/^\s*[-+*]\s+/, "• ");
    line = line.replace(/^\s*\d+[.)]\s+/, "• ");
    line = line.replace(/^\s*\|(.*)\|\s*$/, (_, row) => row.trim().split("|").map((cell) => cell.trim()).join(" · "));
    line = line.replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1");
    line = line.replace(/\[([^\]]*)\]\([^)]*\)/g, "$1");
    line = line.replace(/`+([^`]*?)`+/g, "$1");
    line = line.replace(/(\*\*|__)(.*?)\1/g, "$2");
    line = line.replace(/(\*|_)(.*?)\1/g, "$2");
    line = line.replace(/~~(.*?)~~/g, "$1");
    line = line.replace(/<[^>]*>/g, "");
    line = line.replace(/[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}]/gu, "");
    // Plain-device mode intentionally drops residual Markdown punctuation.
    line = line.replace(/[#*_`~|>]/g, "");
    line = line.replace(/^---+\s*$/, "");
    lines.push(line.replace(/\s+$/, ""));
  }
  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
