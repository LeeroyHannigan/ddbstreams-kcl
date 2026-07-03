'use strict';

// Proves the Node auto-download path end to end against a local http server:
// stable-asset naming, SHA-256 verification, and cache write. No network, no
// real release required.

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const { discoverSidecar, platformArch, VERSION } = require('../src/sidecar');

function listen(server) {
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));
}

test('auto-download round trip', async () => {
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), 'ddbsc-cache-'));
  const { osName, arch, ext } = platformArch();
  const asset = `amazon-dynamodb-streams-consumer-sidecar-${osName}-${arch}${ext}`;
  const body = Buffer.from('#!/bin/sh\necho fake-sidecar\n');
  const sum = crypto.createHash('sha256').update(body).digest('hex');

  const routes = {
    [`/v${VERSION}/${asset}`]: body,
    [`/v${VERSION}/${asset}.sha256`]: Buffer.from(`${sum}  ${asset}\n`),
  };
  const server = http.createServer((req, res) => {
    if (routes[req.url]) {
      res.writeHead(200);
      res.end(routes[req.url]);
    } else {
      res.writeHead(404);
      res.end('not found');
    }
  });
  const port = await listen(server);

  const prevBase = process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE;
  const prevCache = process.env.XDG_CACHE_HOME;
  const prevSidecar = process.env.DDB_STREAMS_CONSUMER_SIDECAR;
  process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE = `http://127.0.0.1:${port}`;
  process.env.XDG_CACHE_HOME = cache;
  delete process.env.DDB_STREAMS_CONSUMER_SIDECAR;

  try {
    const p = await discoverSidecar();
    assert.ok(p.startsWith(cache), `expected cache under ${cache}, got ${p}`);
    assert.deepStrictEqual(fs.readFileSync(p), body);
  } finally {
    server.close();
    if (prevBase === undefined) delete process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE;
    else process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE = prevBase;
    if (prevCache === undefined) delete process.env.XDG_CACHE_HOME;
    else process.env.XDG_CACHE_HOME = prevCache;
    if (prevSidecar !== undefined) process.env.DDB_STREAMS_CONSUMER_SIDECAR = prevSidecar;
  }
});

test('checksum mismatch fails (no PATH fallback)', async () => {
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), 'ddbsc-cache-'));
  const { osName, arch, ext } = platformArch();
  const asset = `amazon-dynamodb-streams-consumer-sidecar-${osName}-${arch}${ext}`;
  const body = Buffer.from('real');
  const routes = {
    [`/v${VERSION}/${asset}`]: body,
    [`/v${VERSION}/${asset}.sha256`]: Buffer.from(`deadbeef  ${asset}\n`),
  };
  const server = http.createServer((req, res) => {
    if (routes[req.url]) {
      res.writeHead(200);
      res.end(routes[req.url]);
    } else {
      res.writeHead(404);
      res.end('nf');
    }
  });
  const port = await listen(server);

  const prev = {
    base: process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE,
    cache: process.env.XDG_CACHE_HOME,
    p: process.env.PATH,
  };
  process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE = `http://127.0.0.1:${port}`;
  process.env.XDG_CACHE_HOME = cache;
  process.env.PATH = fs.mkdtempSync(path.join(os.tmpdir(), 'ddbsc-emptypath-'));

  try {
    await assert.rejects(discoverSidecar(), /checksum mismatch|could not obtain/);
  } finally {
    server.close();
    process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE = prev.base;
    process.env.XDG_CACHE_HOME = prev.cache;
    process.env.PATH = prev.p;
  }
});
