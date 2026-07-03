'use strict';

// Drives every shared conformance fixture (conformance/fixtures/*.json) through
// the real Worker against the shared replay_sidecar.py -- no AWS, no real
// sidecar. Mirrors clients/python/tests/test_conformance.py and clients/go.

const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { Worker } = require('../src/index');

const CONF = path.join(__dirname, '..', '..', '..', 'conformance');
const REPLAY = path.join(CONF, 'replay_sidecar.py');
const FIX_DIR = path.join(CONF, 'fixtures');

function collector() {
  return {
    byShard: {},
    ended: [],
    processRecords(records) {
      for (const r of records) {
        (this.byShard[r.shardId] ||= []).push(r.sequenceNumber);
      }
    },
    shardEnded(shardId) {
      this.ended.push(shardId);
    },
  };
}

const fixtures = fs.readdirSync(FIX_DIR).filter((f) => f.endsWith('.json'));
assert.ok(fixtures.length > 0, `no fixtures under ${FIX_DIR}`);

for (const fx of fixtures) {
  const fpath = path.join(FIX_DIR, fx);
  const fixture = JSON.parse(fs.readFileSync(fpath, 'utf8'));
  test(`conformance: ${fixture.name}`, async () => {
    const c = collector();
    const w = new Worker({
      streamArn: 'arn:aws:dynamodb:us-east-1:1:table/T/stream/2026',
      leaseTable: 'leases',
      processor: c,
      sidecarCmd: ['python3', REPLAY, fpath],
    });
    const code = await w.run();

    // Checkpointing: replay exits non-zero on a wrong/absent ack.
    assert.strictEqual(code, 0, `${fixture.name}: replay rejected checkpoint acks (exit ${code})`);

    // Delivery: counts + per-shard order.
    const counts = Object.fromEntries(Object.entries(c.byShard).map(([k, v]) => [k, v.length]));
    assert.deepStrictEqual(counts, fixture.expect.records_per_shard, `${fixture.name}: records_per_shard`);
    for (const [shard, order] of Object.entries(fixture.expect.record_order)) {
      assert.deepStrictEqual(c.byShard[shard] || [], order, `${fixture.name}: order ${shard}`);
    }

    // Lifecycle: shard_ended.
    assert.deepStrictEqual(
      [...c.ended].sort(),
      [...fixture.expect.shard_ended].sort(),
      `${fixture.name}: shard_ended`
    );
  });
}
