# drizzle - development notes

Author: Agnish Ghosh <iamagnix@gmail.com>

Running design log, written alongside the code. Newest entries are at the
bottom of each section. "Open question" / "TODO" markers are deliberate.

---

## 1. What we're building

A **blob feed relayer**: it watches Ethereum L1 for EIP-4844 blob-carrying
transactions (type 3) that belong to one or more L2s, fetches the blobs from a
beacon node, decodes them using the L2's own blob encoding, and fans the result
out to any number of consumers over a streaming HTTP API.

```
            ┌──────────────┐  eth_getBlockByNumber   ┌─────────────────────────┐
            │ EL JSON-RPC  │◄────────────────────────┤                         │
            └──────────────┘                         │   drizzle               │
            ┌──────────────┐  /eth/v1/beacon/blobs   │                         │
            │ Beacon REST  │◄────────────────────────┤ head ─► sched ─► N wkrs │
            └──────────────┘  (or blob_sidecars)     │            ─► reorder   │
                                                     │            ─► feed hub  │
                                                     └──────────┬──────────────┘
                                         chunked HTTP stream    │ (binary or ndjson)
                                 ┌──────────────┬───────────────┴───┐
                              base-indexer   arb-indexer   any consumer
```

"Pluggable L2s" = an L2 is described by an **adapter**: which L1 address
receives its batches (inbox), optionally which senders are trusted batchers,
and a **decode** proc that turns the tx's raw blobs into L2 batch payload
bytes. Built-in kinds: `opstack` (Base, OP Mainnet, any OP-stack chain),
`arbitrum` (Arbitrum One/Nova, Orbit chains), `raw` (no decoding).
Adding an L2 family = one new module under `drizzle/l2/` that calls
`registerAdapterKind`.

Interpretation note: "relayer" here means L1 → consumers (read side). It does
**not** post batches to L1 (that would be a batcher/poster). If the
write-side is wanted later, the adapter interface is the place to add an
`encode` hook (both codecs already have encoders, used by tests).

## 2. Coding standards (Nimbus / status-im)

Following the conventions used in nimbus-eth2:

- every module starts with the dual MIT/Apache-2.0 header + `{.push raises: [].}`
- errors are values: `Result[T, E]` / `Opt[T]` from `results`; exceptions from
  stdlib/chronos are caught at the boundary and converted
- chronos v4 typed async: `{.async: (raises: [CancelledError]).}`
- `func` for anything side-effect free
- chronicles structured logging, message starts with capital letter, fields
  as key = value, no string interpolation in messages
- spec constants keep spec casing (`BYTES_PER_BLOB`), everything else Nim casing
- `--styleCheck:error` is on, so identifier casing is enforced by the compiler
- imports: `std/[...]` first, then third-party, then local

## 3. Build system

Decision (per user): **git submodules + Makefile is primary**, nimble is a
supported secondary path, not a requirement.

- `vendor/*` - pinned submodules: chronos, chronicles, results, stew,
  nimcrypto, confutils, unittest2 and their transitive deps
  (bearssl, http-utils, serialization, json-serialization, faststreams,
  testutils).
- `vendor/Nim` - the compiler itself, pinned at v2.2.12, shallow submodule.
  Needed because current chronos/bearssl/confutils require Nim >= 2.2.10 and
  the system Nim was 2.2.0 (chronos failed with a raises-tracking error under
  2.2.0). `make nim` bootstraps it (csources_v2 -> koch boot), about 2 minutes.
  `make NIM=nim` skips it if a new enough Nim is on PATH.
- `config.nims` adds every `vendor/*` dir to the path and sets `--noNimblePath`,
  **unless** `NimblePkgVersion` is defined (nimble always passes it), in which
  case nimble owns the search path. That one switch is what lets both build
  paths share one config.
- Gotcha: nim-bearssl has nested submodules (C sources); `make update` runs
  `git submodule update --init --recursive`. Without it bearssl's patch step
  asserts.

## 4. Data path & throughput design

Goals: keep up with L1 at max blob target with lots of headroom, catch up
from a historical start block quickly, and serve many consumers without one
slow consumer hurting the rest.

1. **EL-driven, not beacon-driven.** We walk EL blocks
   (`eth_getBlockByNumber(n, true)`), filter type-3 txs by `to` through a hash
   index (`Table[Address, seq[L2Adapter]]`, O(1) per tx), and only touch the beacon node for
   blocks that actually contain a matching tx. Most blocks cost exactly one
   RPC call.
2. **Slot from timestamp.** `slot = (timestamp - genesis_time) / SECONDS_PER_SLOT`
   (same trick op-node uses) avoids a beacon block lookup per EL block.
3. **Parallel fetch, ordered emit.** A scheduler hands block numbers to a pool
   of N workers (`--workers`, default 16); results land in a reorder buffer
   and are emitted strictly in L1 order. The scheduler window is bounded
   (`workers * 4` blocks ahead of the emit cursor) so the reorder buffer can't
   grow without bound when one block is slow.
4. **Keep-alive connection pooling** via one chronos `HttpSessionRef` per
   upstream, so parallel workers reuse TCP connections.
5. **Zero-copy fan-out.** Blobs are `ref array[131072, byte]` (`BlobRef`),
   shared by every event and subscriber. Hex is decoded straight into the
   ref array (no intermediate `seq`). Each event's wire encoding is computed
   **once, lazily, per format** and cached on the event, so N subscribers
   cost N socket writes, not N encodes.
6. **Backpressure without head-of-line blocking.** Every subscriber has a
   bounded queue (`--subscriber-queue`). Publishing never awaits: if a
   subscriber's queue is full it gets a `lagged` frame and is dropped. The
   client reconnects with `?from=<lastSeq+1>` and is replayed from the
   per-feed ring buffer (`--ring-size`) if it's still in range.
7. **Binary framing by default** (length-prefixed), ndjson for debugging.
   Raw blobs are opt-in per subscriber (`?blobs=1`): decoded payloads are
   ~0.99x the blob size anyway, so sending both doubles bandwidth for nothing
   unless the consumer wants to verify.

## 5. Reorg handling

Every emitted block's `(number, hash)` is remembered (last 256). When the next
block's `parentHash` doesn't match, we:

1. bump a `generation` counter, so in-flight worker results become stale and are
   dropped on arrival;
2. publish a `reorg` event on every feed with `rewindTo = n - 2`
   (meaning: discard everything you got from L1 blocks > rewindTo);
3. rewind the scheduler + reorder buffer to `n - 1` and refetch.

If the reorg is deeper, refetching `n - 1` mismatches against `n - 2` and the
same thing happens again, so the rewind walks back one block at a time.
That's fine because L1 reorgs are almost always 1-2 blocks.
`--confirmations` (default 0) lets operators trade latency for never seeing
reorgs in practice.

## 6. Blob retrieval: `blobs` vs `blob_sidecars`

Since Fulu/PeerDAS (mainnet Dec 2025), beacon nodes are moving to
`GET /eth/v1/beacon/blobs/{block_id}?versioned_hashes=...`, which returns
plain blobs. The old `blob_sidecars` endpoint is still around on many
nodes. `--beacon-api=auto|blobs|sidecars`:

- `auto` tries `blobs`, falls back to `sidecars` if the endpoint isn't served
  (400/404/405/501) and remembers which one worked.
- Ordering trick for `blobs`: we know the block's full ordered list of
  versioned hashes from the EL block (blob index = position across all type-3 txs in
  tx order). We request the wanted hashes **sorted by block index**, so the
  response is correct whether the server returns them in request order or
  in block order.
- For `sidecars` we request `?indices=` and **verify**
  `kzg_to_versioned_hash(commitment) == expected` for each one.

**TODO (security), opt-in:** neither path verifies blob ↔ commitment
with a KZG proof by default; we trust the beacon node. Planned as an opt-in
`--verify-kzg` flag (off by default, per the 2026-11-10 decision): vendor
`c-kzg-4844` (or `nim-kzg4844`) and, when the flag is set, run
`verify_blob_kzg_proof_batch` for sidecars, or compute the commitment for
`blobs` responses. The cost only applies when asked for. Without the flag,
run drizzle against a beacon node you control.

## 7. L2 blob encodings

### OP-stack (`opstack`)
Per op-service `eth/blob.go`: version byte 0 at offset 1, 3-byte BE length at
[2..4], data packed 4 field elements → 127 bytes per round (31 bytes per FE
+ 4×6 spare bits reassembled into 3 bytes), 1024 rounds,
`MaxBlobDataSize = (4*31+3)*1024 - 4 = 130044`. Decoder validates that the
top 2 bits of each FE's first byte are 0 and that all trailing bytes are 0.
Each blob decodes independently → one payload per blob (each is a
derivation-version-prefixed frame stream).

### Arbitrum Nitro (`arbitrum`)
Per nitro `util/blobs/blobs.go`: per blob, bytes 1..31 of every FE
(126976 bytes) **followed by** the low 6 spare bits of every FE's byte 0,
packed LSB-first (3072 bytes): 130048 bytes per blob. Concatenate across
**all blobs of the tx**, then RLP-decode a single byte string. One payload
per tx; it starts with Nitro's batch header byte (`0x00` = brotli). We only
need an RLP *string* decoder (`spec/rlp_lite.nim`), not general RLP.

### Presets
Addresses as known at time of writing. **Verify against each chain's official
docs before production use.** Batcher addresses can be rotated via
SystemConfig (OP) / rollup admin (Arbitrum), which is why batcher filtering
is optional and why custom feeds can be passed on the CLI.

---
- 2026-11-10: ~~dependency pin: results v0.5.0~~ **retracted, see the
  correction entry below.** I'd concluded chronos 4.4.1 breaks on results
  0.5.1, but the tests were really running against chronos *master*.
- 2026-11-10: core modules written bottom-up, type-checking each layer before
  the next: spec → l2 codecs/adapters → rpc → feed → pipeline → server → main.
  The full binary built first time via `make`.
- 2026-11-10: gotchas hit along the way, recorded so nobody hits them twice:
  - case-object discriminant enums must start at 0, so `FeedEventKind` is
    0-based and the wire byte is `ord + 1`.
  - `let x = GlobalRef` read from async code isn't gcsafe; the hub allocates
    a fresh `Lagged` event per drop instead (it's tiny).
  - `OrderedTable` has no `withValue`; use `getOrDefault` + nil check.
  - `seq.add Foo.fromHex(x).valueOr: ...` parses as command syntax and breaks;
    bind to a `let` first.
  - `std/times` and chronos both export `seconds`, `milliseconds`, `Duration`;
    benches import `std/times except ...`.
  - chronos `resp.send(seq)` copies the seq into the async closure. The stream
    loop uses the `(pointer, len)` overload into the cached frame and keeps
    `ev` referenced across the await, so fan-out is copy-free.
- 2026-11-10: tests: 37 unit + e2e tests (`make test`). The e2e test runs
  the real relayer and API against an in-process mock EL + beacon, covering
  decoding, batcher filtering, blob-index offsets, a reorg, the poll and
  binary-stream endpoints, and the blob_sidecars fallback with commitment
  checks.
- 2026-11-10: **correction: submodule pins weren't actually pinned.** I'd
  checked out the tags inside each submodule but never `git add`ed the
  gitlinks, so the superproject still recorded each repo's default-branch
  HEAD from `git submodule add`. Running `git submodule update --init
  --recursive` (to fetch bearssl's C sources) silently reset *every*
  submodule to master. So the "results 0.5.1 breaks chronos" bisect was
  really testing **chronos master** (where the `toException` mixin clash in
  `datagram.nim` is real) against each results tag. Found because nimble
  resolved results 0.5.1 and built fine. Fix: re-checked out all tags,
  `git add vendor .gitmodules`, verified each staged gitlink equals
  `rev-parse <tag>^{commit}`. Real pins: chronos v4.4.1 + results **v0.5.1**,
  which builds clean, passes all tests, and also compiles chronos's own
  testdatagram. nimble file is back to `results >= 0.5.1`.
  **Rule: after changing a submodule checkout, always `git add vendor/<name>`
  before running any `git submodule update`.**
- 2026-11-10: KZG verification decided as **opt-in** (user call): behind a
  `--verify-kzg` flag, off by default. Rationale: it costs CPU on the hot
  path (batch proof verification per blob), and the usual deployment is
  drizzle next to an operator-controlled beacon node, where the trust is
  already there. Sections 6 and 10 updated.
- 2026-11-10: **first live mainnet run** (PublicNode EL + beacon, no
  local nodes): base and optimism decoded fine; **arbitrum failed** with
  "rlp: long string out of bounds". Cause: the codec ignored Nitro's
  spare-bit section (6 bits/FE, 3072 bytes/blob). The synthetic tests passed
  because my encoder and decoder shared the same wrong model; round-trip
  tests can't catch a spec misread. Fixed per nitro source (checked:
  `spareBlobBits = 6`). Re-ran from the failing block 26383147: two 3-blob
  batches decoded to ~387 KB each, starting with 0x00 (brotli header), and
  0 decode errors over 29 blocks.
  **Lesson: every codec needs at least one real-chain vector.** TODO: commit
  a real mainnet blob as a test fixture (can't fetch one from a script here,
  PublicNode 403s non-browser UAs and spoofing is off the table; drizzle's
  own client is fine, so dump one via `?blobs=1`).
- 2026-11-10: spec links added as module doc comments in each codec /
  spec module (OP ecotone blob-encoding spec, nitro blobs.go + SequencerInbox,
  EIP-4844, RLP, beacon-APIs). All URLs checked to return 200.

## 9. Throughput: measurements

`make bench` (release build, one core, Linux x86-64, this dev box):

| stage                                   | blobs/s | MB/s  |
|-----------------------------------------|--------:|------:|
| hex → blob (table-driven, from bytes)   |  13,300 | 1,750 |
| OP-stack decode                         |  43,300 | 5,680 |
| Arbitrum decode                         | 198,000 | 26,000|
| wire encode binary+blob (once per event)|  17,300 | 4,540 |

End-to-end catch-up (300 L1 blocks × 6 blobs, relayer **and** mock upstream
sharing one core, so this is a lower bound):

| upstream RTT | workers | blocks/s | blobs/s | blob MB/s |
|--------------|--------:|---------:|--------:|----------:|
| 0            | 16      | 155      | 935     | 123       |
| 25 ms        | 1       | 17       | 103     | 13        |
| 25 ms        | 4       | 51       | 311     | 41        |
| 25 ms        | 16      | 101      | 611     | 80        |
| 25 ms        | 64      | 139      | 838     | 110       |

Mainnet max (21 blobs / 12 s) is 1.75 blobs/s, so live-following is a non-issue;
the numbers matter for **catch-up/backfill** and for remote RPC providers
where latency, not CPU, dominates (hence `--workers`).

Optimisation log, with what did and didn't help:

1. Baseline e2e: ~470 blobs/s at every worker count, i.e. CPU-bound.
2. Table-driven, branch-free hex decode: 885 → 1,750 MB/s on the micro
   benchmark, but e2e barely moved, so this wasn't the bottleneck.
3. Zero-copy `/blobs` scanner (`scanBlobsResponse`): hex is decoded straight
   from the HTTP body bytes, with no std/json tree and no 262 KiB string node
   per blob. Falls back to std/json on anything unexpected. e2e → ~580.
4. A bigger HTTP read buffer (16 KiB → 1 MiB) made **no difference**. A raw
   chronos client/server loopback probe moves about 500 MB/s with either size,
   so the transport wasn't it either.
5. **The real culprit was the benchmark itself**: every slot is requested
   once, so the mock's response cache never hit, and the mock re-serialized
   1.5 MB of JSON per block on the shared core. With responses prewarmed
   outside the timed region: e2e → ~930 blobs/s (re-measured on the
   corrected chronos v4.4.1 pin: 935). Lesson: profile the harness
   before optimizing the code. (perf was unavailable, perf_event_paranoid=4.)

Where the next 2x would come from:
- move hex decode + L2 decode to a thread pool (nim-taskpools, as Nimbus
  does); `DecodeProc` is already pure and `{.noSideEffect.}` for this reason
- SSZ (`Accept: application/octet-stream`) from the beacon API where
  supported: no hex at all, half the bytes on the wire
- `eth_getBlockByNumber(n, false)` + `eth_getTransactionByHash` only for
  blob txs, or `eth_getBlockReceipts` filtering, to cut EL JSON for busy blocks

## 10. Open items / next steps

- [ ] Optional KZG verification behind `--verify-kzg` (off by default,
      section 6): vendor c-kzg-4844 as a submodule; only linked into the
      hot path when the flag is set.
- [ ] Beacon-API-compatible serving (`/eth/v1/beacon/blob_sidecars/{id}`
      from cache) so op-node / nitro can point `--l1.beacon` at drizzle
      unchanged. Needs proofs + inclusion proofs on sidecars, so it depends on
      the KZG item (would require `--verify-kzg` / the KZG library).
- [ ] Persistence: ring buffer is in-memory only. A small on-disk log (or
      SQLite like Nimbus' slashing DB) would allow replay across restarts.
- [ ] nim-metrics / Prometheus endpoint (Nimbus convention) instead of the
      JSON `/v1/status` counters.
- [ ] WebSocket `newHeads` subscription to cut head-detection latency vs
      polling.
- [ ] Write side: optional batch-posting mode using the codecs' encoders.
- [ ] Verify preset addresses against official docs; consider loading
      presets from a TOML file so new chains don't need a rebuild.
