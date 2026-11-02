import test from "node:test";
import assert from "node:assert/strict";
import { flattenMarkdownForDevice } from "./device-markdown.js";

test("device Markdown flattening keeps readable text without syntax", () => {
  const source = "## 标题\n\n**粗体中文**\n\n- 列表一\n- 列表二\n\n`code`\n\n[链接](https://example.com)";
  assert.equal(flattenMarkdownForDevice(source), "标题\n\n粗体中文\n\n• 列表一\n• 列表二\n\ncode\n\n链接");
});

test("device Markdown flattening preserves fenced code content and line breaks", () => {
  const source = "说明\n\n```js\nconst x = 1;\n**raw**\n```\n结束";
  assert.equal(flattenMarkdownForDevice(source), "说明\n\nconst x = 1;\n**raw**\n结束");
});
