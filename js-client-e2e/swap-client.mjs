// Long-lived official @rocicorp/zero client that survives hot server swaps.
// Phases are advanced by marker files created by the orchestrator:
//   <CTRL>/phase2, <CTRL>/phase3, <CTRL>/done
// At each phase the script: waits for reconnect, waits until a phase-specific
// upstream row (inserted while the client was disconnected or after swap)
// becomes visible, then issues a CRUD mutation and waits to see it applied.
// Exits 0 with a JSON summary, nonzero on any failure or fatal client error.
import {Zero, createBuilder, createSchema, number, string, boolean, table} from '@rocicorp/zero';
import {existsSync} from 'node:fs';

const cacheURL = process.env.SWAP_CACHE_URL;
const ctrl = process.env.SWAP_CTRL_DIR;
if (!cacheURL || !ctrl) throw new Error('SWAP_CACHE_URL and SWAP_CTRL_DIR required');

const schema = createSchema({
  enableLegacyQueries: true,
  tables: [
    table('issue')
      .columns({
        id: string(),
        title: string(),
        owner: string(),
        open: boolean(),
        rank: number(),
      })
      .primaryKey('id'),
  ],
});
const zql = createBuilder(schema);

const diagnostics = [];
const fatal = [];
const lastCookieByClient = new Map();
const states = [];
const log = m => process.stderr.write(`[swap-client] ${m}\n`);

const logSink = {
  log(level, context, ...args) {
    const msg = `${level} ${JSON.stringify(context)} ${args.map(a => {
      if (a instanceof Error) return a.stack ?? a.message;
      try { return JSON.stringify(a); } catch { return String(a); }
    }).join(' ')}`;
    diagnostics.push(msg);
    // Track poke cookies for monotonicity across server swaps.
    for (const arg of args) {
      if (!Array.isArray(arg) || arg[0] !== 'pokeEnd') continue;
      const clientID = context?.clientID;
      const cookie = arg[1]?.cookie;
      if (typeof clientID !== 'string' || typeof cookie !== 'string') continue;
      const prev = lastCookieByClient.get(clientID);
      if (prev !== undefined && cookie < prev) {
        fatal.push(`nonMonotonicCookie: ${cookie} after ${prev}`);
      }
      lastCookieByClient.set(clientID, cookie);
    }
  },
};

const zero = new Zero({
  cacheURL,
  userID: 'swap-user',
  schema,
  kvStore: 'mem',
  logLevel: 'debug',
  logSink,
  pingTimeoutMs: 1_000,
  disconnectTimeoutMs: 300_000,
  onUpdateNeeded: r => fatal.push(`onUpdateNeeded: ${JSON.stringify(r)}`),
  onClientStateNotFound: () => fatal.push('onClientStateNotFound'),
});
zero.connection.state.subscribe(s => {
  states.push(s.name);
  log(`connection: ${s.name}${s.reason ? ` (${JSON.stringify(s.reason)})` : ''}`);
});

const sleep = ms => new Promise(r => setTimeout(r, ms));
async function waitFor(pred, label, ms = 60_000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const v = await pred();
    if (v) return v;
    await sleep(50);
  }
  throw new Error(`timed out waiting for ${label}`);
}

// One live materialized view over the whole table, kept open across swaps —
// pokes flow into it. `type: 'complete'` queries would re-request; the open
// view proves live incremental sync across implementations.
const view = zero.materialize(zero.query.issue.orderBy('id', 'asc'));
let rows = [];
view.addListener(data => { rows = [...data]; });

const rowById = id => rows.find(r => r.id === id);

async function phaseCheck(name, markerRow, liveId, liveTitle) {
  log(`--- ${name}: waiting for connected`);
  await waitFor(() => {
    const st = zero.connection.state.current.name;
    // After disconnectTimeoutMs the run loop parks; nudge it to resume.
    if (st === 'disconnected' || st === 'error') zero.connection.connect();
    return st === 'connected';
  }, `${name} connected`, 120_000);
  log(`${name}: connected; waiting for upstream row ${markerRow}`);
  await waitFor(() => rowById(markerRow), `${name} upstream row ${markerRow} poked to client`);
  log(`${name}: marker row visible; waiting for live update ${liveId} -> "${liveTitle}"`);
  await waitFor(() => rowById(liveId)?.title === liveTitle, `${name} live update ${liveId} poked`);
  log(`${name}: OK (rows=${rows.length})`);
}

try {
  await waitFor(() => zero.connection.state.current.name === 'connected', 'initial connect', 120_000);
  await waitFor(() => rows.length >= 1000, 'initial hydration of 1000 rows');
  log(`phase1: hydrated ${rows.length} rows`);
  log('phase1: OK — signaling ready');
  const {writeFileSync} = await import('node:fs');
  writeFileSync(`${ctrl}/phase1-done`, '');

  await waitFor(() => existsSync(`${ctrl}/phase2`), 'phase2 marker', 300_000);
  await phaseCheck('phase2 (rust)', 'swap-p2', 'i2', 'phase2-rust');
  writeFileSync(`${ctrl}/phase2-done`, '');

  await waitFor(() => existsSync(`${ctrl}/phase3`), 'phase3 marker', 300_000);
  await phaseCheck('phase3 (official again)', 'swap-p3', 'i3', 'phase3-official');
  writeFileSync(`${ctrl}/phase3-done`, '');

  if (fatal.length) throw new Error(`fatal diagnostics:\n${fatal.join('\n')}`);
  console.log(JSON.stringify({
    ok: true,
    finalRowCount: rows.length,
    states,
    finalCookies: Object.fromEntries(lastCookieByClient),
  }));
  process.exit(0);
} catch (err) {
  process.stderr.write(`FAILED: ${err?.stack ?? err}\n\nfatal: ${fatal.join('\n')}\n\nlast diagnostics:\n${diagnostics.slice(-40).join('\n')}\n`);
  process.exit(1);
}
