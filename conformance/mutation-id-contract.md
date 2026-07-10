# Mutation replay and ordering contract

This contract is pinned to upstream `zero/v1.7.0`
(`6863de5f00a3c1e7dc09c83ea3263dec4a94ebee`). It is based on:

- `packages/zero-server/src/process-mutations.ts` —
  `#checkAndIncrementLastMutationID`;
- `packages/zero-server/src/push-processor.pg.test.ts` — `previously seen
  mutation` and `stops processing mutations as soon as it hits an out of order
  mutation`.

It applies to the upstream application-side `PushProcessor` transaction, not
to a UI-level interpretation of a log line.

## Per mutation

The transaction computes the next expected ID (`stored lastMutationID + 1`) and
compares the received ID to it.

| received ID | upstream result | durable LMID |
| --- | --- | --- |
| equal to expected | apply/confirm mutation | advances to that ID |
| less than expected | `alreadyProcessed`; do not reapply | unchanged (the speculative increment rolls back) |
| greater than expected | out-of-order failure | unchanged (whole transaction rolls back) |

For a stale replay, the per-mutation response is:

```json
{
  "id": {"clientID": "cid", "id": 100},
  "result": {
    "error": "alreadyProcessed",
    "details": "Ignoring mutation from cid with ID 100 as it was already processed. Expected: 101"
  }
}
```

The diagnostic’s `Expected` number is the next ID at the time of that
transaction. Replays do **not** advance it. Therefore, seeing ID 100 expected
101 and later ID 89 expected 105 means accepted mutations advanced the durable
counter from 100 to 104 in between; it is not valid to attribute that change to
the old/replayed IDs.

## Batch behavior

- A replay-only batch continues over stale mutations and returns an
  `alreadyProcessed` result for each one; none of those items advances the
  durable LMID.
- On the first ID greater than expected, upstream stops processing the batch.
  It returns a top-level `error` message whose body is:

  ```json
  {
    "kind": "pushFailed",
    "origin": "server",
    "reason": "oooMutation",
    "message": "Client cid sent mutation ID 5 but expected 3",
    "mutationIDs": [
      {"clientID": "cid", "id": 5},
      {"clientID": "cid", "id": 4}
    ]
  }
  ```

  `mutationIDs` contains the offending mutation and the remaining **unprocessed**
  mutations, not items successfully handled before the gap.

The black-box WebSocket transcript corpus cannot exercise this app-side
transaction with the default benchmark stack because that stack intentionally
has no application mutate endpoint. The Rust executor is covered directly by
the live Postgres regression in `zero-cache-mutagen::apply_mutation`, which
asserts that repeated stale IDs retain a stable expected counter. When wiring a
real mutate endpoint into a differential environment, add this vector to its
push-response transcript unchanged rather than normalizing its `details` or
`mutationIDs` fields.
