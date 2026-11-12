# drizzle

High-throughput EIP-4844 **blob feed relayer** with pluggable L2 adapters.

drizzle follows Ethereum L1, picks out the blob transactions that belong to
the L2s you configure (Base, OP Mainnet, Arbitrum One, or any OP-stack /
Arbitrum Orbit chain), fetches their blobs from a beacon node, decodes them
with each L2's own blob encoding, and streams the result to any number of
consumers over HTTP.

## Architecture

```
   Ethereum L1
   +------------------+      +------------------+
   | Execution node   |      | Beacon node      |
   | (blocks, txs)    |      | (blobs)          |
   +--------+---------+      +---------+--------+
            |                          |
            v                          v
   +-------------------------------------------+
   |                 drizzle                   |
   |                                           |
   |   1. Track      follow new L1 blocks      |
   |   2. Filter     keep blob txs sent to a   |
   |                 configured L2 inbox       |
   |   3. Fetch      get only those blobs      |
   |   4. Decode     L2 adapter (pluggable)    |
   |   5. Order      emit in L1 order,         |
   |                 signal reorgs             |
   |   6. Fan out    one feed per L2           |
   +---------------------+---------------------+
                         |  HTTP: stream / poll
          +--------------+--------------+
          v              v              v
     L2 consumer    L2 consumer    L2 consumer
```

Steps 1-4 run in parallel across blocks; step 5 puts results back in order.
Each L2 is just an adapter (inbox address, optional batcher list, and a
decode function), so supporting a new chain doesn't touch the pipeline.
Design details are in [NOTES.md](NOTES.md).

## Throughput

Measured with `make bench` (release build, a single core on an x86-64 Linux dev box):

| stage (per blob)                        | blobs/s | MB/s   |
|-----------------------------------------|--------:|-------:|
| hex → blob (JSON hot path)              |  13,200 |  1,730 |
| OP-stack decode                         |  44,100 |  5,780 |
| Arbitrum decode                         | 133,500 | 17,500 |
| wire encode, binary + blob (once/event) |  17,100 |  4,490 |

End to end, catching up 300 L1 blocks × 6 blobs through the full pipeline
against a mock EL + beacon node:

| upstream RTT | workers | blocks/s | blobs/s | blob MB/s |
|--------------|--------:|---------:|--------:|----------:|
| 0            | 16      | 154      | 926     | 122       |
| 25 ms        | 1       | 17       | 103     | 13        |
| 25 ms        | 4       | 51       | 309     | 41        |
| 25 ms        | 16      | 100      | 600     | 79        |
| 25 ms        | 64      | 139      | 834     | 109       |

The end-to-end figures are a lower bound, because the mock upstream shares
the relayer's core. Mainnet's maximum is 21 blobs per 12 s block, about
1.75 blobs/s, so following the chain live uses a tiny fraction of capacity.
The throughput matters when backfilling. Against remote RPC providers,
where latency dominates, raise `--workers`.

## Build

Dependencies are git submodules under `vendor/`, including the Nim compiler,
and the Makefile is the primary build:

```sh
git clone --recursive <repo> drizzle && cd drizzle   # or: make update
make nim        # bootstrap vendored Nim 2.2.12 (once, ~2 min)
make            # -> build/drizzle
make test       # unit + end-to-end tests
make bench      # throughput benchmarks
```

If Nim >= 2.2.10 is already on your PATH: `make NIM=nim`.
nimble also works: `nimble build` / `nimble test`, using `drizzle.nimble`.

## Run

```sh
./build/drizzle \
  --el-url=http://localhost:8545 \
  --beacon-url=http://localhost:5052 \
  --feed=base --feed=arbitrum-one \
  --feed=mychain:opstack:0xINBOX:0xBATCHER
```

Beacon endpoints:

- `--beacon-url` is repeatable, in priority order. An endpoint that fails or
  rate-limits is backed off (Retry-After is respected) and the next one is
  used.
- At startup each endpoint is checked: it must be on the same network and
  must actually serve full blobs. Since PeerDAS, only a node that stores at
  least half the blob data can, e.g. a supernode.
- Beacon nodes keep blobs for about 18 days. To backfill further, add
  `--beacon-archive-url` (any beacon-API-compatible blob archive). Without
  one, drizzle stops at the first pruned block and says why. It never
  silently skips a batch.

`--feed` takes a preset name (`base`, `optimism`, `arbitrum-one`) or
`<name>:<kind>:<inbox>[:<batcher>,...]`, where kind is `opstack`, `arbitrum`
or `raw`. Leave out batchers to accept any sender. Run `--help` for all options
(workers, confirmations, start block, ring size and so on).

> Preset addresses were correct when written, but batchers rotate. Verify
> them against each chain's official docs before production use.

## Dashboard

Open `http://localhost:7700/` while drizzle is running. The page is built into
the binary (no CDN, works offline) and shows:

- pipeline position: L1 head, safe head, last emitted block, how far behind
- blocks/s, blobs/s and decoded MB/s (12 s average), totals, errors, reorgs
- 5-minute throughput charts
- per-feed table: events, decoded bytes, subscribers, lagged drops
- a live event tail per feed, with reorgs highlighted

It only uses the public API (`/v1/status` and `format=summary` streams), so
you can host a copy elsewhere and point it at any drizzle instance.

## Consume

| endpoint | what |
|---|---|
| `GET /v1/feeds` | configured feeds |
| `GET /v1/status` | pipeline and per-feed counters |
| `GET /` | dashboard |
| `GET /v1/feeds/{name}/stream?from=&format=bin\|json\|summary&blobs=0\|1` | live chunked stream with replay (`summary` = sizes only, no payload bytes) |
| `GET /v1/feeds/{name}/events?from=&limit=&blobs=0\|1` | JSON polling |

```sh
curl -N 'localhost:7700/v1/feeds/base/stream?format=json'
```

Each event carries the L1 block, slot, tx hash, sender, versioned hashes and the
decoded payloads, plus raw blobs if you ask for `blobs=1`. On an L1 reorg every feed
receives a `Reorg` event with `rewindTo`: discard anything from L1 blocks
after it. A consumer that falls behind gets a `Lagged` frame and is
disconnected; reconnect with `from=<lastSeq+1>` to resume from the replay
ring. The binary frame layout is documented in [drizzle/feed/wire.nim](drizzle/feed/wire.nim),
and `decodeFrame` there is a reference decoder.

## Adding an L2 family

1. Write a pure decoder `proc(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string]`.
2. Call `registerAdapterKind("mykind", myDecoder)` in a module that's linked in
   (see [drizzle/l2/builtin.nim](drizzle/l2/builtin.nim)).
3. Use it: `--feed=chain:mykind:0xINBOX`.

## Layout

```
drizzle/spec      EIP-4844 types, hex, minimal RLP
drizzle/l2        adapter interface, OP-stack + Arbitrum codecs, presets
drizzle/rpc       pooled JSON-RPC / beacon REST clients
drizzle/pipeline  head tracking, scheduler, workers, reorder, reorgs
drizzle/feed      events, wire format, fan-out hub, HTTP API, dashboard
tests/            unit tests + mock EL/beacon end-to-end tests
bench/            throughput benchmarks
```

See [NOTES.md](NOTES.md) for design decisions, measurements and open items.
Blobs are not KZG-verified by default; drizzle trusts its beacon node, so
point it at one you run. Opt-in verification (`--verify-kzg`) is on the roadmap.
