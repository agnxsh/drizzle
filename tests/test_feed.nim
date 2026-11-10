# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.used.}

import
  std/strutils,
  unittest2, chronos,
  ../drizzle/feed/[hub, wire],
  ../drizzle/l2/presets,
  ../drizzle/pipeline/reorder

func batch(n: uint64, withBlob = false): FeedEvent =
  result = FeedEvent(
    kind: FeedEventKind.Batch, l1Number: n, l1Timestamp: 1000 + n, slot: n * 2,
    txHash: Hash32.fromHex("0x" & "ab".repeat(32)).get,
    l1Hash: Hash32.fromHex("0x" & "cd".repeat(32)).get,
    sender: Address.fromHex("0x" & "ef".repeat(20)).get,
    versionedHashes: @[VersionedHash.fromHex("0x01" & "00".repeat(31)).get],
    payloads: @[@[1'u8, 2, 3], @[]])
  if withBlob:
    let b = new Blob
    b[0] = 7; b[BYTES_PER_BLOB - 1] = 9
    result.blobs = @[b]

suite "Reorder buffer":
  test "releases strictly in order":
    var b = ReorderBuffer[string].init(10)
    check b.push(12, "c")
    check b.popReady().isNone
    check b.push(10, "a")
    check b.push(11, "b")
    check not b.push(11, "dup")
    check b.popReady().get == "a"
    check b.popReady().get == "b"
    check b.popReady().get == "c"
    check b.popReady().isNone
    check not b.push(5, "stale")
    b.reset(3)
    check b.len == 0 and b.next == 3

suite "Wire format":
  test "binary round trip, with and without blobs":
    for withBlobs in [false, true]:
      let ev = batch(42, withBlob = true)
      ev.seqNo = 7
      let frame = ev.encode(wireVariant(false, withBlobs))
      let (back, used) = decodeFrame(frame).get
      check:
        used == frame.len
        back.kind == FeedEventKind.Batch
        back.seqNo == 7
        back.l1Number == 42 and back.slot == 84 and back.l1Timestamp == 1042
        back.txHash == ev.txHash and back.sender == ev.sender
        back.versionedHashes == ev.versionedHashes
        back.payloads == ev.payloads
        back.blobs.len == (if withBlobs: 1 else: 0)
      if withBlobs:
        check back.blobs[0][] == ev.blobs[0][]

  test "reorg / heartbeat frames":
    let r = FeedEvent(kind: FeedEventKind.Reorg, seqNo: 3, rewindTo: 99)
    let (back, _) = decodeFrame(r.encode(WireVariant.Binary)).get
    check back.kind == FeedEventKind.Reorg and back.rewindTo == 99
    let h = FeedEvent(kind: FeedEventKind.Heartbeat)
    check decodeFrame(h.encode(WireVariant.Binary)).get[0].kind == FeedEventKind.Heartbeat

  test "encodings are cached per variant":
    let ev = batch(1)
    let a = unsafeAddr ev.encode(WireVariant.Binary)[0]
    let b = unsafeAddr ev.encode(WireVariant.Binary)[0]
    check a == b
    check ev.encode(WireVariant.Json).len > 0
    check ev.encode(WireVariant.Json)[^1] == byte('\n')

  test "truncated and garbage frames are rejected":
    let frame = batch(1).encode(WireVariant.Binary)
    check decodeFrame(frame[0 ..< frame.len - 1]).isErr
    check decodeFrame(frame[0 ..< 3]).isErr
    var bad = frame
    bad[4] = 99   # version
    check decodeFrame(bad).isErr

  test "back-to-back frames decode sequentially":
    var stream: seq[byte]
    for i in 1'u64 .. 3:
      let ev = batch(i)
      ev.seqNo = i
      stream.add ev.encode(WireVariant.Binary)
    var pos = 0
    var seen: seq[uint64]
    while pos < stream.len:
      let (ev, used) = decodeFrame(stream.toOpenArray(pos, stream.high)).get
      seen.add ev.seqNo
      pos += used
    check seen == @[1'u64, 2, 3]

suite "Feed hub":
  setup:
    let feed = Feed.new(parseFeedSpec("base").get, ringSize = 4, queueSize = 2)

  test "assigns sequence numbers and keeps a bounded ring":
    for i in 1'u64 .. 6: feed.publish(batch(i))
    check feed.headSeq == 6
    check feed.oldestSeq == 3
    check feed.eventsSince(0, 100).len == 4
    check feed.eventsSince(5, 100).len == 2
    check feed.eventsSince(0, 1).len == 1

  test "live subscriber receives published events":
    let sub = feed.subscribe(Opt.none(uint64)).get
    feed.publish(batch(1))
    let ev = waitFor sub.queue.popFirst()
    check ev.seqNo == 1 and ev.feed == "base"

  test "replay from the ring, then live":
    for i in 1'u64 .. 3: feed.publish(batch(i))
    let sub = feed.subscribe(Opt.some(2'u64)).get
    feed.publish(batch(4))
    var got: seq[uint64]
    while not sub.queue.empty:
      got.add (waitFor sub.queue.popFirst()).seqNo
    check got == @[2'u64, 3, 4]

  test "evicted start is rejected":
    for i in 1'u64 .. 6: feed.publish(batch(i))
    check feed.subscribe(Opt.some(1'u64)).isErr
    check feed.subscribe(Opt.some(3'u64)).isOk
    check feed.subscribe(Opt.some(7'u64)).isOk  # next event: fine

  test "slow subscriber is dropped with a Lagged frame, others unaffected":
    let
      slow = feed.subscribe(Opt.none(uint64)).get
      fast = feed.subscribe(Opt.none(uint64)).get
    for i in 1'u64 .. 2:
      feed.publish(batch(i))
      discard waitFor fast.queue.popFirst()
    feed.publish(batch(3))   # slow's queue (size 2) is full -> dropped
    check slow.closed
    check slow.queue.len == 1
    check (waitFor slow.queue.popFirst()).kind == FeedEventKind.Lagged
    check (waitFor fast.queue.popFirst()).seqNo == 3
    check feed.subscriberCount == 1
    check feed.stats.laggedDrops == 1
