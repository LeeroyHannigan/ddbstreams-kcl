'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const https = require('node:https');
const crypto = require('node:crypto');

const BINARY = 'amazon-dynamodb-streams-consumer-sidecar';
const VERSION = '0.1.0';
const RELEASE_BASE =
  process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE ||
  'https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/download';

// Maps Node's platform/arch to the stable release asset naming (os in
// {linux,darwin,windows}, arch in {x86_64,aarch64}) -- the same language-neutral
// contract release.yml publishes and the Go client consumes.
function platformArch() {
  const osName =
    process.platform === 'win32' ? 'windows' : process.platform === 'darwin' ? 'darwin' : 'linux';
  const arch = process.arch === 'x64' ? 'x86_64' : process.arch === 'arm64' ? 'aarch64' : process.arch;
  const ext = osName === 'windows' ? '.exe' : '';
  return { osName, arch, ext };
}

function cachePath() {
  const base = process.env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache');
  const { ext } = platformArch();
  return path.join(base, 'amazon-dynamodb-streams-consumer', VERSION, BINARY + ext);
}

// GET with redirect following (GitHub release assets 302 to a CDN).
function httpGet(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('too many redirects'));
    https
      .get(url, (res) => {
        const { statusCode, headers } = res;
        if (statusCode >= 300 && statusCode < 400 && headers.location) {
          res.resume();
          resolve(httpGet(headers.location, redirects + 1));
          return;
        }
        if (statusCode !== 200) {
          res.resume();
          reject(new Error(`GET ${url}: HTTP ${statusCode}`));
          return;
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
        res.on('error', reject);
      })
      .on('error', reject);
  });
}

async function download(dst) {
  const { osName, arch, ext } = platformArch();
  const asset = `${BINARY}-${osName}-${arch}${ext}`;
  const binURL = `${RELEASE_BASE.replace(/\/$/, '')}/v${VERSION}/${asset}`;
  const want = (await httpGet(binURL + '.sha256')).toString().trim().split(/\s+/)[0];
  const body = await httpGet(binURL);
  const got = crypto.createHash('sha256').update(body).digest('hex');
  if (got.toLowerCase() !== want.toLowerCase()) {
    throw new Error(`checksum mismatch for ${asset}: got ${got} want ${want}`);
  }
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  const tmp = `${dst}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, body, { mode: 0o755 });
  fs.renameSync(tmp, dst);
  return dst;
}

function onPath() {
  const { ext } = platformArch();
  const name = BINARY + ext;
  for (const dir of (process.env.PATH || '').split(path.delimiter)) {
    if (!dir) continue;
    const p = path.join(dir, name);
    try {
      fs.accessSync(p, fs.constants.X_OK);
      return p;
    } catch {
      /* not here */
    }
  }
  return null;
}

// Resolution order: explicit path -> env override -> cached download -> download
// -> PATH. Unlike npm, we don't ship a per-platform binary in the tarball; the
// sidecar is fetched once and cached, so it is still install-and-go.
async function discoverSidecar(explicit) {
  if (explicit) return explicit;
  if (process.env.DDB_STREAMS_CONSUMER_SIDECAR) return process.env.DDB_STREAMS_CONSUMER_SIDECAR;
  const cached = cachePath();
  try {
    fs.accessSync(cached, fs.constants.X_OK);
    return cached;
  } catch {
    /* need to fetch */
  }
  try {
    return await download(cached);
  } catch (e) {
    const p = onPath();
    if (p) return p;
    throw new Error(
      `could not obtain the ${BINARY} sidecar: download failed (${e.message}) and it is not on PATH. ` +
        'Set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar or install it manually'
    );
  }
}

module.exports = { discoverSidecar, cachePath, platformArch, VERSION };
