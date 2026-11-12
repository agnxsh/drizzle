# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Throughput benchmarks:
##   1. codec micro-benchmarks (hex, OP-stack, Arbitrum, wire encode)
##   2. end-to-end catch-up: mock EL + beacon on loopback -> full pipeline
##
## The e2e number is a lower bound: the mock upstream runs on the same
## single-threaded event loop as the relayer, so it competes for the CPU
## (JSON encoding 262 KiB of hex per blob on the "server" side as well).

import std/times except seconds, milliseconds, Duration
import
  std/[monotimes, strutils, os],
  chronos, chronicles, stew/byteutils,
  ../drizzle/spec/eip4844,
  ../drizzle/l2/[presets, opstack_codec, arbitrum_codec],
  ../drizzle/rpc/[el_client, beacon_client],
  ../drizzle/feed/[hub, wire],
  ../drizzle/pipeline/relayer,
  ../tests/mock_upstream

template timeIt(label: string, iters: int, bytesPerIter: int, body: untyped) =
  let t0 = getMonoTime()
  for _ in 0 ..< iters:
    body
  let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9
  echo "  ", label.alignLeft(36),
    ($int(iters.float / secs)).align(8), " blobs/s",
    ($int(bytesPerIter.float * iters.float / secs / 1e6)).align(7), " MB/s"

proc microBenchmarks() =
  echo "== codecs, single core =="
  var data = newSeq[byte](OpMaxBlobDataSize)
  for i in 0 ..< data.len: data[i] = byte(i * 7)
  let
    opBlob = encodeOpBlob(data).get
    hex = opBlob[].to0xHex()
    arbBlobs = encodeArbitrumBlobs(data[0 ..< 120_000])

  var sink: int
  timeIt("hex -> blob (JSON hot path)", 2000, BYTES_PER_BLOB):
    sink += blobFromHex(hex).get[0].int
  timeIt("opstack decode", 2000, BYTES_PER_BLOB):
    sink += decodeOpBlob(opBlob[]).get.len
  timeIt("opstack encode", 2000, BYTES_PER_BLOB):
    sink += encodeOpBlob(data).get[0].int
  timeIt("arbitrum decode", 2000, BYTES_PER_BLOB):
    sink += decodeArbitrumBlobs(arbBlobs).get.len

  let ev = FeedEvent(kind: FeedEventKind.Batch, payloads: @[data],
                     versionedHashes: @[default(VersionedHash)], blobs: @[opBlob])
  timeIt("wire encode binary+blob (uncached)", 2000, 2 * BYTES_PER_BLOB):
    ev.encoded[WireVariant.BinaryWithBlobs].setLen(0)
    sink += ev.encode(WireVariant.BinaryWithBlobs).len
  doAssert sink != 0

proc e2e(blocks, blobsPerBlock, workers: int, latency: Duration) =
  const
    Inbox = "0xff00000000000000000000000000000000008453"
    Batcher = "0x5050f69a9786f081509234f1a7f4684b5e5b76c9"
  echo "  ", blocks, " blocks x ", blobsPerBlock, " blobs, upstream RTT ",
    latency, ", ", workers, " workers:"

  # Share a few blob buffers across the whole fake chain to keep memory flat.
  var pool: seq[BlobRef]
  for i in 0 ..< blobsPerBlock:
    var d = newSeq[byte](100_000)
    for j in 0 ..< d.len: d[j] = byte(i + j)
    pool.add encodeOpBlob(d).get

  let chain = MockChain(latency: latency, cacheResponses: true)
  for n in 1'u64 .. uint64(blocks):
    chain.setBlock(n, 0, 0, [MockTx(sender: Batcher, to: Inbox, blobs: pool)])
  chain.head = uint64(blocks)
  chain.prewarmBlobResponses()

  let (mock, url) = startMock(chain)
  let adapters = @[parseFeedSpec("base").get]
  let
    hub = FeedHub.new(adapters, ringSize = 64, queueSize = 64)
    el = ElClient.new(url, 30.seconds)
    beacon = BeaconClient.new(url, 30.seconds, BeaconApiMode.Blobs)
  doAssert (waitFor beacon.init()).isOk
  let relayer = Relayer.new(
    RelayerConfig(workers: workers, windowPerWorker: 4,
                  pollInterval: 5.milliseconds, startBlock: Opt.some(1'u64),
                  retainBlobs: false),
    el, beacon, AdapterIndex.init(adapters).get, hub)

  let t0 = getMonoTime()
  relayer.start()
  while relayer.stats.nextEmit <= uint64(blocks):
    waitFor sleepAsync(2.milliseconds)
  let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9

  let
    blobs = blocks * blobsPerBlock
    mb = blobs.float * BYTES_PER_BLOB.float / 1e6
  echo "      ", $int(blocks.float / secs), " blocks/s, ",
    $int(blobs.float / secs), " blobs/s, ",
    formatFloat(mb / secs, ffDecimal, 1), " MB/s blob data (",
    formatFloat(secs, ffDecimal, 2), "s)"

  waitFor relayer.stop()
  waitFor el.http.closeWait()
  waitFor beacon.closeWait()
  waitFor mock.stop()
  waitFor mock.closeWait()

when isMainModule:
  setLogLevel(LogLevel.WARN)
  microBenchmarks()
  let blocks = if paramCount() >= 1: parseInt(paramStr(1)) else: 300
  echo "== end-to-end catch-up (relayer + mock upstream on one core) =="
  e2e(blocks, 6, 16, ZeroDuration)
  for w in [1, 4, 16, 64]:
    e2e(blocks, 6, w, 25.milliseconds)
  echo "  (mainnet max with 21 blobs/block is 1.75 blobs/s)"
