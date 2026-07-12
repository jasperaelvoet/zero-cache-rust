import {
  Zero,
  createBuilder,
  createSchema,
  defineMutator,
  defineMutators,
  defineQueries,
  defineQuery,
  number,
  string,
  table,
} from '@rocicorp/zero';

const cacheURL = process.env.ZERO_JS_CLIENT_CACHE_URL;
const secondCacheURL = process.env.ZERO_JS_CLIENT_CACHE_URL_2 ?? cacheURL;
if (!cacheURL) {
  throw new Error('ZERO_JS_CLIENT_CACHE_URL is required');
}

// The reject timer must be cleared once the race settles: a pending setTimeout
// keeps the Node event loop alive, so a stray 20s timer used to hold every
// otherwise-finished run (and its Rust test) open for the full 20s.
const timeout = (promise, label, ms = 10_000) => {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
};

const waitFor = async (predicate, label, ms = 10_000) => {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const value = predicate();
    if (value) return value;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${label}`);
};

const schema = createSchema({
  tables: [
    table('issue')
      .columns({
        id: number(),
        title: string(),
        owner: string(),
      })
      .primaryKey('id'),
  ],
});
const zql = createBuilder(schema);
const queries = defineQueries({
  issuesForOwner: defineQuery(({args}) =>
    zql.issue.where('owner', '=', args.owner).orderBy('id', 'asc'),
  ),
});
const mutators = defineMutators({
  issue: {
    rename: defineMutator(({tx, args}) =>
      tx.mutate.issue.update({id: args.id, title: args.title}),
    ),
  },
});

const diagnostics = [];
const fatalDiagnostics = [];
const lastCookieByClient = new Map();
const expectedAppError = 'expected-js-e2e-rejection';
const stringify = value => {
  if (value instanceof Error) return value.stack ?? value.message;
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};
const recordFatal = (kind, value) => {
  const message = `${kind}: ${stringify(value)}`;
  fatalDiagnostics.push(message);
  diagnostics.push(message);
};
process.on('unhandledRejection', reason => recordFatal('unhandledRejection', reason));
process.on('uncaughtException', error => recordFatal('uncaughtException', error));

const logSink = {
  log(level, context, ...args) {
    const message = `${level} ${stringify(context)} ${args.map(stringify).join(' ')}`;
    diagnostics.push(message);
    if (level === 'error' && !message.includes(expectedAppError)) {
      fatalDiagnostics.push(message);
    }
    for (const arg of args) {
      if (!Array.isArray(arg) || arg[0] !== 'pokeEnd') continue;
      const clientID = context?.clientID;
      const cookie = arg[1]?.cookie;
      if (typeof clientID !== 'string' || typeof cookie !== 'string') continue;
      const previous = lastCookieByClient.get(clientID);
      if (previous !== undefined && cookie <= previous) {
        recordFatal(
          'nonMonotonicCookie',
          `client ${clientID} received ${cookie} after ${previous}`,
        );
      }
      lastCookieByClient.set(clientID, cookie);
    }
  },
};

let zero;
let zero2;
let view;
try {
  const storageKey = `full-client-${Date.now()}-${Math.random()}`;
  const makeZero = serverURL => new Zero({
    cacheURL: serverURL,
    userID: 'js-e2e-user',
    storageKey,
    schema,
    mutators,
    kvStore: 'mem',
    logLevel: 'debug',
    logSink,
    pingTimeoutMs: 1_000,
    disconnectTimeoutMs: 5_000,
    onUpdateNeeded: reason => recordFatal('onUpdateNeeded', reason),
    onClientStateNotFound: () => recordFatal('onClientStateNotFound', 'called'),
  });
  zero = makeZero(cacheURL);

  const states = [zero.connection.state.current];
  const unsubscribeState = zero.connection.state.subscribe(state => {
    states.push(state);
    if (state.name === 'error' || state.name === 'needs-auth') {
      recordFatal(`connection:${state.name}`, state.reason);
    }
  });

  await waitFor(
    () => zero.connection.state.current.name === 'connected',
    'Zero connection',
  );

  view = zero.materialize(queries.issuesForOwner({owner: 'alice'}));
  const viewEvents = [];
  const removeListener = view.addListener((data, resultType, error) => {
    viewEvents.push({data: structuredClone(data), resultType, error});
    if (error) recordFatal('queryError', error);
  });

  const initial = await timeout(
    zero.run(queries.issuesForOwner({owner: 'alice'}), {type: 'complete'}),
    'complete custom query',
  );
  if (JSON.stringify(initial) !== JSON.stringify([
    {id: 1, title: 'alpha', owner: 'alice'},
    {id: 3, title: 'gamma', owner: 'alice'},
  ])) {
    throw new Error(`unexpected custom-query rows: ${JSON.stringify(initial)}`);
  }
  await waitFor(
    () => viewEvents.some(event => event.resultType === 'complete'),
    'materialized query completeness',
  );

  // A second Zero instance with the same storage identity models another tab
  // or an overlapping reconnect. Both clients belong to one client group and
  // race durable CVR transitions, which is essential for catching optimistic
  // concurrency failures in the server's persistence path.
  zero2 = makeZero(secondCacheURL);
  const states2 = [zero2.connection.state.current];
  let unsubscribeState2 = zero2.connection.state.subscribe(state => {
    states2.push(state);
    if (state.name === 'error' || state.name === 'needs-auth') {
      recordFatal(`connection2:${state.name}`, state.reason);
    }
  });
  await waitFor(
    () => zero2.connection.state.current.name === 'connected',
    'second Zero connection',
  );

  const concurrentQueries = [];
  for (let nonce = 0; nonce < 8; nonce++) {
    concurrentQueries.push(
      zero.run(
        queries.issuesForOwner({owner: 'alice', nonce}),
        {type: 'complete'},
      ),
      zero2.run(
        queries.issuesForOwner({owner: 'bob', nonce}),
        {type: 'complete'},
      ),
    );
  }
  await timeout(Promise.all(concurrentQueries), 'concurrent custom queries', 20_000);

  // Deterministic randomized churn covers overlapping put/del query lifetimes
  // and many more CVR transitions than the minimal happy path. Keeping the
  // seed fixed makes a failure replayable.
  let randomState = 0x5eed1234;
  const random = () => {
    randomState = (Math.imul(randomState, 1664525) + 1013904223) >>> 0;
    return randomState / 0x1_0000_0000;
  };
  for (let round = 0; round < 6; round++) {
    const batch = [];
    for (let index = 0; index < 8; index++) {
      const owner = random() < 0.5 ? 'alice' : 'bob';
      const client = random() < 0.5 ? zero : zero2;
      batch.push(client.run(
        queries.issuesForOwner({owner, stress: {round, index, salt: randomState}}),
        {type: 'complete'},
      ).then(rows => ({owner, clientID: client.clientID, rows})));
    }
    const results = await timeout(Promise.all(batch), `random query batch ${round}`, 20_000);
    for (const result of results) {
      const expectedCount = result.owner === 'alice' ? 2 : 1;
      if (result.rows.length !== expectedCount ||
          result.rows.some(row => row.owner !== result.owner)) {
        throw new Error(`invalid randomized query result in round ${round}: ${JSON.stringify(result)}`);
      }
    }
  }

  const mutation = zero.mutate(
    mutators.issue.rename({id: 1, title: 'renamed optimistically'}),
  );
  const mutation2 = zero2.mutate(
    mutators.issue.rename({id: 3, title: 'gamma renamed optimistically'}),
  );
  await timeout(
    Promise.all([mutation.client, mutation2.client]),
    'optimistic mutations',
  );
  await waitFor(
    () => view.data.some(row => row.id === 1 && row.title === 'renamed optimistically'),
    'optimistic row in live query',
  );
  const serverResults = await timeout(
    Promise.all([mutation.server, mutation2.server]),
    'custom mutation server results',
  );
  if (serverResults.some(result => result?.type !== 'success')) {
    throw new Error(`unexpected mutation server results: ${JSON.stringify(serverResults)}`);
  }

  // Application-level rejection must settle only the rejected mutation. It
  // must not poison the connection, lose subsequent pokes, or trigger a retry
  // loop that resubmits the mutation indefinitely.
  const rejectedMutation = zero.mutate(
    mutators.issue.rename({id: 1, title: 'reject-me-on-purpose'}),
  );
  await timeout(rejectedMutation.client, 'rejected optimistic mutation');
  let rejection;
  try {
    rejection = await timeout(rejectedMutation.server, 'application mutation error');
  } catch (error) {
    rejection = error;
  }
  if (!stringify(rejection).includes(expectedAppError)) {
    throw new Error(`application mutation error was not surfaced: ${stringify(rejection)}`);
  }

  // Tear down and recreate the second official client while the first remains
  // live. The shared in-memory store preserves its cookie, exercising the
  // reconnect/resume base-cookie path with another client concurrently active.
  unsubscribeState2();
  await zero2.close();
  zero2 = makeZero(secondCacheURL);
  unsubscribeState2 = zero2.connection.state.subscribe(state => {
    states2.push(state);
    if (state.name === 'error' || state.name === 'needs-auth') {
      recordFatal(`reconnectedConnection2:${state.name}`, state.reason);
    }
  });
  await waitFor(
    () => zero2.connection.state.current.name === 'connected',
    'reconnected second Zero client',
  );

  // Force another query lifecycle after the mutation response. This is the
  // sequence that exposes stale/regressing poke cookies in real applications.
  const bobRows = await timeout(
    zero2.run(queries.issuesForOwner({owner: 'bob'}), {type: 'complete'}),
    'post-mutation custom query',
  );
  if (JSON.stringify(bobRows) !== JSON.stringify([
    {id: 2, title: 'beta', owner: 'bob'},
  ])) {
    throw new Error(`unexpected post-mutation rows: ${JSON.stringify(bobRows)}`);
  }

  await new Promise(resolve => setTimeout(resolve, 250));
  if (zero.connection.state.current.name !== 'connected') {
    throw new Error(
      `client did not remain connected: ${JSON.stringify(zero.connection.state.current)}`,
    );
  }
  if (fatalDiagnostics.length) {
    throw new Error(`Zero client diagnostics contained fatal errors:\n${fatalDiagnostics.join('\n')}`);
  }

  removeListener();
  unsubscribeState();
  unsubscribeState2();
  console.log(JSON.stringify({
    ok: true,
    clientID: zero.clientID,
    secondClientID: zero2.clientID,
    states: states.map(state => state.name),
    states2: states2.map(state => state.name),
    completeViewEvents: viewEvents.filter(event => event.resultType === 'complete').length,
    finalCookies: Object.fromEntries(lastCookieByClient),
  }));
} catch (error) {
  process.stderr.write(`${stringify(error)}\n\nZero diagnostics:\n${diagnostics.join('\n')}\n`);
  process.exitCode = 1;
} finally {
  view?.destroy();
  await zero2?.close();
  await zero?.close();
}
