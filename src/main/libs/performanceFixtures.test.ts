import fs from 'fs';
import { createRequire } from 'module';
import os from 'os';
import path from 'path';
import { afterEach, expect, test } from 'vitest';

const require = createRequire(import.meta.url);
const {
  generatePerformanceFixtures,
} = require('../../../scripts/generate-performance-fixtures.cjs') as {
  generatePerformanceFixtures: (outputDir: string, options?: { messageCount?: number }) => {
    outputDir: string;
    outputPath: string;
    messageCount: number;
    highFrequencyEventCount: number;
  };
};

let tempDir: string | null = null;

afterEach(() => {
  if (tempDir) {
    fs.rmSync(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

test('generates reusable performance fixtures outside real user data', () => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wesight-performance-fixtures-'));
  const result = generatePerformanceFixtures(tempDir, { messageCount: 12 });

  expect(result.outputDir).toBe(tempDir);
  expect(result.messageCount).toBe(12);
  expect(result.highFrequencyEventCount).toBe(250);
  expect(result.outputPath).toContain(tempDir);

  const payload = JSON.parse(fs.readFileSync(result.outputPath, 'utf8')) as {
    session: { messages: unknown[] };
    longMarkdown: string;
    longToolLog: string;
    largeDiff: string;
    mermaid: string;
    highFrequencyStream: unknown[];
  };
  expect(payload.session.messages).toHaveLength(12);
  expect(payload.longMarkdown).toContain('Long Markdown Fixture');
  expect(payload.longToolLog).toContain('[tool] streamed log output');
  expect(payload.largeDiff).toContain('diff --git');
  expect(payload.mermaid).toContain('flowchart TD');
  expect(payload.highFrequencyStream).toHaveLength(250);
});
