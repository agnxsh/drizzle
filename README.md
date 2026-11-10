# drizzle

High-throughput EIP-4844 **blob feed relayer** with pluggable L2 adapters.

drizzle follows Ethereum L1, picks out the blob transactions that belong to
the L2s you configure (Base, OP Mainnet, Arbitrum One, or any OP-stack /
Arbitrum Orbit chain), fetches their blobs from a beacon node, decodes them
with each L2's own blob encoding, and streams the result to any number of
consumers over HTTP.


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

`--feed` takes a preset name (`base`, `optimism`, `arbitrum-one`) or
`<name>:<kind>:<inbox>[:<batcher>,...]`, where kind is `opstack`, `arbitrum`
or `raw`. Leave out batchers to accept any sender. Run `--help` for all options
(workers, confirmations, start block, ring size and so on).

> Preset addresses were correct when written, but batchers rotate. Verify
> them against each chain's official docs before production use.

## Consume

| endpoint | what |
|---|---|
| `GET /v1/feeds` | configured feeds |
| `GET /v1/status` | pipeline and per-feed counters |
| `GET /v1/feeds/{name}/stream?from=&format=bin\|json&blobs=0\|1` | live chunked stream with replay |
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
drizzle/feed      events, wire format, fan-out hub, HTTP API
tests/            unit tests + mock EL/beacon end-to-end tests
bench/            throughput benchmarks
```

See [NOTES.md](NOTES.md) for design decisions, measurements and open items.
Blobs are not KZG-verified by default; drizzle trusts its beacon node, so
point it at one you run. Opt-in verification (`--verify-kzg`) is on the roadmap.
