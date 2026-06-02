import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, expect, test } from 'vitest';
import yauzl from 'yauzl';

import { exportLogsZip } from './logExport';

let tempDir: string | null = null;

afterEach(() => {
  if (tempDir) {
    fs.rmSync(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

function readZipEntries(zipPath: string): Promise<Record<string, string>> {
  return new Promise((resolve, reject) => {
    const entries: Record<string, string> = {};
    yauzl.open(zipPath, { lazyEntries: true }, (openError, zipFile) => {
      if (openError || !zipFile) {
        reject(openError);
        return;
      }
      zipFile.readEntry();
      zipFile.on('entry', (entry) => {
        zipFile.openReadStream(entry, (streamError, stream) => {
          if (streamError || !stream) {
            reject(streamError);
            return;
          }
          const chunks: Buffer[] = [];
          stream.on('data', (chunk: Buffer) => chunks.push(chunk));
          stream.on('end', () => {
            entries[entry.fileName] = Buffer.concat(chunks).toString('utf8');
            zipFile.readEntry();
          });
        });
      });
      zipFile.on('end', () => resolve(entries));
      zipFile.on('error', reject);
    });
  });
}

test('exports file entries, missing placeholders, and buffer entries', async () => {
  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wesight-log-export-'));
  const logPath = path.join(tempDir, 'main.log');
  const zipPath = path.join(tempDir, 'logs.zip');
  fs.writeFileSync(logPath, 'log content');

  const result = await exportLogsZip({
    outputPath: zipPath,
    entries: [
      { archiveName: 'main.log', filePath: logPath },
      { archiveName: 'missing.log', filePath: path.join(tempDir, 'missing.log') },
    ],
    bufferEntries: [{
      archiveName: 'performance-snapshot.json',
      buffer: Buffer.from(JSON.stringify({ ok: true }), 'utf8'),
    }],
  });

  expect(result.missingEntries).toEqual(['missing.log']);
  const entries = await readZipEntries(zipPath);
  expect(entries['main.log']).toBe('log content');
  expect(entries['missing.log']).toBe('');
  expect(JSON.parse(entries['performance-snapshot.json'])).toEqual({ ok: true });
});
