#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_MESSAGE_COUNT = 1000;

function repeatLine(line, count) {
  return Array.from({ length: count }, (_, index) => `${line} ${index + 1}`).join('\n');
}

function buildLargeDiff() {
  const removed = repeatLine('- old implementation line', 240);
  const added = repeatLine('+ new implementation line', 240);
  return `diff --git a/src/example.ts b/src/example.ts\nindex 1111111..2222222 100644\n--- a/src/example.ts\n+++ b/src/example.ts\n@@ -1,240 +1,240 @@\n${removed}\n${added}\n`;
}

function buildMessages(count = DEFAULT_MESSAGE_COUNT) {
  const now = Date.now();
  return Array.from({ length: count }, (_, index) => {
    const isUser = index % 2 === 0;
    return {
      id: `fixture-message-${index + 1}`,
      type: isUser ? 'user' : 'assistant',
      content: isUser
        ? `Fixture prompt ${index + 1}`
        : `Fixture assistant response ${index + 1}\n\n${repeatLine('Generated markdown paragraph', 3)}`,
      timestamp: now + index,
      metadata: isUser ? undefined : { fixture: true },
    };
  });
}

function generatePerformanceFixtures(outputDir, options = {}) {
  const messageCount = Number.isFinite(options.messageCount)
    ? Math.max(1, Math.floor(options.messageCount))
    : DEFAULT_MESSAGE_COUNT;
  fs.mkdirSync(outputDir, { recursive: true });

  const messages = buildMessages(messageCount);
  const payload = {
    generatedAt: new Date().toISOString(),
    session: {
      id: 'fixture-session-1000',
      title: 'Performance Fixture Session',
      status: 'completed',
      cwd: os.tmpdir(),
      messages,
    },
    longMarkdown: [
      '# Long Markdown Fixture',
      '',
      repeatLine('This paragraph is intended to stress markdown rendering.', 180),
      '',
      '```ts',
      repeatLine('const value = computeSomething();', 160),
      '```',
    ].join('\n'),
    longToolLog: repeatLine('[tool] streamed log output', 800),
    largeDiff: buildLargeDiff(),
    mermaid: [
      'flowchart TD',
      '  A[Start] --> B{Heavy render?}',
      '  B -->|Yes| C[Defer rendering]',
      '  B -->|No| D[Render now]',
      '  C --> E[Done]',
      '  D --> E[Done]',
    ].join('\n'),
    highFrequencyStream: Array.from({ length: 250 }, (_, index) => ({
      sessionId: 'fixture-session-1000',
      messageId: 'fixture-stream-message',
      content: `stream chunk ${index + 1}`,
      emittedAtOffsetMs: index * 20,
    })),
  };

  const outputPath = path.join(outputDir, 'performance-fixtures.json');
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
  return {
    outputDir,
    outputPath,
    messageCount,
    highFrequencyEventCount: payload.highFrequencyStream.length,
  };
}

function parseArgs(argv) {
  const outIndex = argv.indexOf('--out');
  const countIndex = argv.indexOf('--messages');
  return {
    outputDir: outIndex >= 0 && argv[outIndex + 1]
      ? path.resolve(argv[outIndex + 1])
      : path.resolve(process.cwd(), '.cowork-temp', 'performance-fixtures'),
    messageCount: countIndex >= 0 && argv[countIndex + 1]
      ? Number(argv[countIndex + 1])
      : DEFAULT_MESSAGE_COUNT,
  };
}

if (require.main === module) {
  const options = parseArgs(process.argv.slice(2));
  const result = generatePerformanceFixtures(options.outputDir, {
    messageCount: options.messageCount,
  });
  console.log(`[PerformanceFixtures] wrote ${result.messageCount} messages to ${result.outputPath}`);
}

module.exports = {
  buildMessages,
  generatePerformanceFixtures,
};
