# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.used.}

## End-to-end: mock EL + beacon -> relayer -> hub -> HTTP API.

import
  std/[json, strutils],
  unittest2, chronos, chronos/apps/http/httpclient, stew/byteutils,
  ../drizzle/l2/[presets, opstack_codec, arbitrum_codec],
  ../drizzle/rpc/[el_client, beacon_client],
  ../drizzle/feed/[hub, server, wire],
  ../drizzle/pipeline/relayer,
  ./mock_upstream

const
  BaseInbox = "0xff00000000000000000000000000000000008453"
  BaseBatcher = "0x5050f69a9786f081509234f1a7f4684b5e5b76c9"
  ArbInbox = "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6"
  Stranger = "0x2222222222222222222222222222222222222222"

func payload(n: int, seed: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n: result[i] = byte((i * 13 + seed) and 0xff)

proc waitUntil(cond: proc(): bool {.gcsafe, raises: [].}, timeout = 10.seconds): bool =
  let deadline = Moment.now() + timeout
  while Moment.now() < deadline:
    if cond(): return true
    try: waitFor sleepAsync(10.milliseconds)
    except CatchableError: discard
  cond()

proc feedEvents(f: Feed): seq[FeedEvent] = f.eventsSince(0, 1000)

type Harness = object
  chain: MockChain
  mocks: seq[HttpServerRef]
  hub: FeedHub
  relayer: Relayer
  api: ApiServer
  el: ElClient
  beacon: BeaconClient
  apiUrl: string

proc setup(
    chain: MockChain, startBlock: uint64,
    live: seq[MockBeacon] = @[newMockBeacon()], archive: seq[MockBeacon] = @[],
    nowSlot = 110'u64, verifyKzg = false): Harness =
  ## Every mock server serves the same chain; the EL comes from the first.
  var
    mocks: seq[HttpServerRef]
    liveUrls, archiveUrls: seq[string]
  for b in live:
    let (m, url) = startMock(chain, b)
    mocks.add m
    liveUrls.add url
  for b in archive:
    let (m, url) = startMock(chain, b)
    mocks.add m
    archiveUrls.add url
  let adapters = @[
    parseFeedSpec("base").get,
    parseFeedSpec("arbitrum-one").get,
    parseFeedSpec("basetap:raw:" & BaseInbox).get]
  let
    index = AdapterIndex.init(adapters).get
    hub = FeedHub.new(adapters, ringSize = 64, queueSize = 64)
    el = ElClient.new(liveUrls[0], 5.seconds)
    beacon = BeaconClient.new(liveUrls, 5.seconds, BeaconApiMode.Auto,
                              archiveUrls = archiveUrls)
  beacon.clock = proc(): uint64 = MockGenesis + MockSlotSecs * nowSlot
  beacon.verifyKzg = verifyKzg
  doAssert (waitFor beacon.init()).isOk
  let relayer = Relayer.new(
    RelayerConfig(workers: 4, windowPerWorker: 4, confirmations: 0,
                  pollInterval: 20.milliseconds,
                  startBlock: Opt.some(startBlock), retainBlobs: true),
    el, beacon, index, hub)
  let api = ApiServer.new(
    initTAddress("127.0.0.1:0"), hub, relayer, 200.milliseconds, 64).get
  api.start()
  relayer.start()
  Harness(chain: chain, mocks: mocks, hub: hub, relayer: relayer, api: api,
          el: el, beacon: beacon, apiUrl: "http://" & $api.localAddress)

proc teardown(h: Harness) =
  waitFor h.relayer.stop()
  waitFor h.api.stop()
  waitFor h.el.http.closeWait()
  waitFor h.beacon.closeWait()
  for m in h.mocks:
    waitFor m.stop()
    waitFor m.closeWait()

proc buildChain(realKzg = false): (MockChain, seq[seq[byte]], seq[byte]) =
  let chain = MockChain(realKzg: realKzg)
  let
    basePayloads = @[payload(100_000, 1), payload(OpMaxBlobDataSize, 2)]
    arbPayload = payload(150_000, 3)
  for n in 100'u64 .. 105:
    var txs: seq[MockTx]
    case n
    of 101:
      # unrelated blob tx first, so base's blobs start at block index 1
      txs.add MockTx(sender: Stranger, to: Stranger, blobs: @[new Blob])
      txs.add MockTx(sender: BaseBatcher, to: BaseInbox, blobs: @[
        encodeOpBlob(basePayloads[0]).get, encodeOpBlob(basePayloads[1]).get])
    of 103:
      txs.add MockTx(sender: Stranger, to: ArbInbox,
                     blobs: encodeArbitrumBlobs(arbPayload))
    of 104:
      # right inbox, wrong sender: only the open "basetap" feed takes it
      txs.add MockTx(sender: Stranger, to: BaseInbox,
                     blobs: @[encodeOpBlob(payload(10, 9)).get])
    else: discard
    chain.setBlock(n, 0, 0, txs)
  chain.head = 105
  (chain, basePayloads, arbPayload)

suite "Relayer end-to-end":
  test "decodes, filters, orders, survives a reorg and serves the API":
    let (chain, basePayloads, arbPayload) = buildChain()
    let h = setup(chain, 100)
    let
      base = h.hub.getFeed("base").get
      arb = h.hub.getFeed("arbitrum-one").get
      tap = h.hub.getFeed("basetap").get

    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)

    # base: exactly the batcher's tx, both blobs decoded
    check base.feedEvents.len == 1
    let b = base.feedEvents[0]
    check:
      b.kind == FeedEventKind.Batch
      b.l1Number == 101
      b.slot == 101
      b.payloads == basePayloads
      b.blobs.len == 2
      b.decodeError.len == 0

    # arbitrum: two blobs -> one payload
    check arb.feedEvents.len == 1
    check arb.feedEvents[0].payloads == @[arbPayload]

    # raw tap on the same inbox sees both senders, no payloads
    check tap.feedEvents.len == 2
    check tap.feedEvents[0].l1Number == 101
    check tap.feedEvents[1].l1Number == 104
    check tap.feedEvents[1].payloads.len == 0

    # --- reorg: 105 is replaced, 106 builds on the new 105 ---
    let reorgPayload = payload(5000, 7)
    chain.setBlock(105, 1, 0, [])
    chain.setBlock(106, 0, 1, [MockTx(sender: BaseBatcher, to: BaseInbox,
                                      blobs: @[encodeOpBlob(reorgPayload).get])])
    chain.head = 106
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 107)
    check h.relayer.stats.reorgs == 1

    let evs = base.feedEvents
    check evs.len == 3
    check evs[1].kind == FeedEventKind.Reorg
    check evs[1].rewindTo == 104
    check evs[2].l1Number == 106
    check evs[2].payloads == @[reorgPayload]
    # every feed is told about the reorg
    check arb.feedEvents[^1].kind == FeedEventKind.Reorg

    # --- HTTP API ---
    let session = HttpSessionRef.new()
    let (status, body) = waitFor session.fetch(
      parseUri(h.apiUrl & "/v1/feeds/base/events?from=1"))
    check status == 200
    let j = parseJson(string.fromBytes(body))
    check j["events"].len == 3
    check j["events"][0]["payloads"][0].getStr == basePayloads[0].to0xHex()
    check j["events"][1]["kind"].getStr == "Reorg"

    let (s404, _) = waitFor session.fetch(parseUri(h.apiUrl & "/v1/feeds/nope/events"))
    check s404 == 404

    # dashboard + status fields it relies on
    let (sDash, dash) = waitFor session.fetch(parseUri(h.apiUrl & "/"))
    check sDash == 200
    check "<title>drizzle relay</title>" in string.fromBytes(dash)
    let (sStat, stat) = waitFor session.fetch(parseUri(h.apiUrl & "/v1/status"))
    check sStat == 200
    let sj = parseJson(string.fromBytes(stat))
    check sj["version"].getStr.len > 0
    check sj["workers"].getInt > 0
    check sj["feeds"]["base"]["published"].getInt == 3

    # binary stream with replay from seq 1
    let req = HttpClientRequestRef.get(
      session, h.apiUrl & "/v1/feeds/base/stream?from=1").get
    let resp = waitFor req.send()
    check resp.status == 200
    let reader = resp.getBodyReader()
    var kinds: seq[FeedEventKind]
    for _ in 0 ..< 3:
      var hdr: array[4, byte]
      waitFor reader.readExactly(addr hdr[0], 4)
      let bodyLen = (int(hdr[0]) shl 24) or (int(hdr[1]) shl 16) or
                    (int(hdr[2]) shl 8) or int(hdr[3])
      var frame = newSeq[byte](4 + bodyLen)
      frame[0 .. 3] = hdr
      waitFor reader.readExactly(addr frame[4], bodyLen)
      let (ev, _) = decodeFrame(frame).get
      kinds.add ev.kind
      if ev.kind == FeedEventKind.Batch and ev.seqNo == 1:
        check ev.payloads == basePayloads
    check kinds == @[FeedEventKind.Batch, FeedEventKind.Reorg, FeedEventKind.Batch]
    waitFor reader.closeWait()
    waitFor resp.closeWait()
    waitFor req.closeWait()
    waitFor session.closeWait()

    teardown(h)

  test "falls back to blob_sidecars and verifies commitments":
    let (chain, basePayloads, _) = buildChain()
    chain.noBlobsEndpoint = true
    let h = setup(chain, 100)
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
    check chain.sidecarRequests > 0
    teardown(h)

  test "reverted txs: dropped for Arbitrum, kept for OP-stack":
    let chain = MockChain()
    let good = payload(150_000, 3)
    for n in 100'u64 .. 103:
      var txs: seq[MockTx]
      case n
      of 101:
        txs.add MockTx(sender: Stranger, to: ArbInbox, reverted: true,
                       blobs: encodeArbitrumBlobs(payload(1000, 4)))
      of 102:
        txs.add MockTx(sender: Stranger, to: ArbInbox,
                       blobs: encodeArbitrumBlobs(good))
      of 103:
        # op-node doesn't look at receipts, so neither do OP-stack feeds
        txs.add MockTx(sender: BaseBatcher, to: BaseInbox, reverted: true,
                       blobs: @[encodeOpBlob(payload(10, 5)).get])
      else: discard
      chain.setBlock(n, 0, 0, txs)
    chain.head = 103
    let h = setup(chain, 100)
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 104)
    let arb = h.hub.getFeed("arbitrum-one").get.feedEvents
    check arb.len == 1
    check arb[0].l1Number == 102
    check arb[0].payloads == @[good]
    check h.hub.getFeed("base").get.feedEvents.len == 1
    check h.relayer.stats.revertedSkipped == 1
    check chain.receiptRequests == 2       # only for the Arbitrum txs
    # the reverted Arbitrum tx's blobs are never fetched
    check h.relayer.stats.blobsFetched == uint64(encodeArbitrumBlobs(good).len + 1)
    teardown(h)

suite "Beacon robustness":
  test "fails over from a dead endpoint and backs off from it":
    let (chain, basePayloads, arbPayload) = buildChain()
    let dead = newMockBeacon()
    dead.failStatus = 503
    let h = setup(chain, 100, live = @[dead, newMockBeacon()])
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
    check h.hub.getFeed("arbitrum-one").get.feedEvents[0].payloads == @[arbPayload]
    check h.beacon.endpoints[0].failures > 0
    check h.relayer.stats.fetchErrors == 0   # failover is not a failed block
    teardown(h)

  test "honours 429 Retry-After instead of hammering":
    let (chain, basePayloads, _) = buildChain()
    let limited = newMockBeacon()
    limited.rateLimitFirst = 2
    limited.retryAfterSecs = 1
    let started = Moment.now()
    let h = setup(chain, 100, live = @[limited])
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check Moment.now() - started >= 900.milliseconds
    check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
    check limited.rateLimited == 2
    # 3 blob-carrying blocks + the 429s + at most one in-flight per worker
    check limited.blobRequests <= 3 + 2 + 4
    teardown(h)

  test "pruned blobs stall the pipeline with an explanation":
    let (chain, _, _) = buildChain()
    let pruned = newMockBeacon()
    pruned.prunedBelowSlot = 1000
    pruned.retentionEpochs = 1          # 4 slots
    let h = setup(chain, 100, live = @[pruned], nowSlot = 200)
    check waitUntil(proc(): bool = h.relayer.stats.stalledAt == 101)
    check "retention" in h.relayer.stats.stallReason
    check "--beacon-archive-url" in h.relayer.stats.stallReason
    check h.relayer.stats.nextEmit == 101
    check h.relayer.stats.fetchErrors == 1   # reported once, not per retry
    teardown(h)

  test "pruned blobs come from the archive, by block root":
    let (chain, basePayloads, arbPayload) = buildChain()
    let pruned = newMockBeacon()
    pruned.prunedBelowSlot = 1000
    pruned.retentionEpochs = 1
    let archive = newMockBeacon()
    archive.sidecarsOnly = true         # blob-archiver style
    archive.retentionEpochs = 1
    let h = setup(chain, 100, live = @[pruned], archive = @[archive], nowSlot = 200)
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
    check h.hub.getFeed("arbitrum-one").get.feedEvents[0].payloads == @[arbPayload]
    check archive.byRootRequests >= 3
    check h.relayer.stats.stalledAt == 0
    teardown(h)

  test "SSZ blobs when offered, JSON when ignored or rejected":
    for mode in [MockSsz.Serve, MockSsz.Ignore, MockSsz.Reject]:
      let (chain, basePayloads, arbPayload) = buildChain()
      let b = newMockBeacon()
      b.ssz = mode
      let h = setup(chain, 100, live = @[b])
      check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
      check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
      check h.hub.getFeed("arbitrum-one").get.feedEvents[0].payloads == @[arbPayload]
      check h.relayer.stats.fetchErrors == 0
      case mode
      of MockSsz.Serve:
        check h.beacon.endpoints[0].ssz == SszSupport.Yes
        check b.sszResponses >= 3
      of MockSsz.Ignore, MockSsz.Reject:
        check h.beacon.endpoints[0].ssz == SszSupport.No
        check b.sszResponses == 0
      teardown(h)

  test "SSZ splitter rejects partial blobs and reports short lists as missing":
    var body = newSeq[byte](2 * BYTES_PER_BLOB)
    body[BYTES_PER_BLOB] = 7
    let two = splitSszBlobs(body, 2)
    check two.isOk and two.get.len == 2 and two.get[1][0] == 7
    check splitSszBlobs(body, 3).error.msg.startsWith("missing")
    check splitSszBlobs(body.toOpenArray(0, body.len - 2), 2).error.kind ==
      HttpFailureKind.Decode

  test "--verify-kzg skips an endpoint serving wrong blobs":
    let (chain, basePayloads, arbPayload) = buildChain(realKzg = true)
    let liar = newMockBeacon()
    liar.corrupt = true
    let h = setup(chain, 100, live = @[liar, newMockBeacon()], verifyKzg = true)
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check h.hub.getFeed("base").get.feedEvents[0].payloads == basePayloads
    check h.hub.getFeed("arbitrum-one").get.feedEvents[0].payloads == @[arbPayload]
    check h.beacon.kzgRejected >= 3            # once per blob-carrying block
    check h.beacon.kzgVerified == h.relayer.stats.blobsFetched
    teardown(h)

  test "--verify-kzg with only a bad endpoint publishes nothing":
    let (chain, _, _) = buildChain(realKzg = true)
    let liar = newMockBeacon()
    liar.corrupt = true
    let h = setup(chain, 100, live = @[liar], verifyKzg = true)
    check waitUntil(proc(): bool = h.relayer.stats.fetchErrors > 0)
    check h.relayer.stats.nextEmit == 101
    check h.relayer.stats.eventsPublished == 0
    check h.beacon.kzgVerified == 0
    teardown(h)

  test "a head slot not served yet is waited for, not an error":
    let (chain, _, _) = buildChain()
    let lagging = newMockBeacon()
    lagging.hiddenSlots = @[104'u64]
    let h = setup(chain, 100, live = @[lagging], nowSlot = 105)
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 104)
    waitFor sleepAsync(200.milliseconds)
    check h.relayer.stats.nextEmit == 104
    lagging.hiddenSlots = @[]
    check waitUntil(proc(): bool = h.relayer.stats.nextEmit == 106)
    check h.relayer.stats.fetchErrors == 0
    teardown(h)

  test "probe ranks endpoints by blob service and drops other networks":
    let (chain, basePayloads, _) = buildChain()
    let partial = newMockBeacon()
    partial.partial = true
    partial.custodyGroups = 4
    let wrongNet = newMockBeacon()
    wrongNet.genesisTime = MockGenesis + 1
    let full = newMockBeacon()
    full.custodyGroups = 128
    var mocks: seq[HttpServerRef]
    var urls: seq[string]
    for b in [partial, wrongNet, full]:
      let (m, url) = startMock(chain, b)
      mocks.add m
      urls.add url
    let beacon = BeaconClient.new(urls, 5.seconds)
    beacon.clock = proc(): uint64 = MockGenesis + MockSlotSecs * 106
    check (waitFor beacon.init()).isOk
    waitFor beacon.probe()
    check:
      beacon.endpoints[0].status == EndpointStatus.Partial
      beacon.endpoints[0].custodyGroups == Opt.some(4'u64)
      beacon.endpoints[1].status == EndpointStatus.WrongNetwork
      beacon.endpoints[2].status == EndpointStatus.Full
      beacon.endpoints[2].resolved == BeaconApiMode.Blobs

    # The full node is preferred even though it's configured last.
    let before = partial.blobRequests
    let blk = (waitFor ElClient.new(urls[0], 5.seconds).getBlock(101)).get.get
    var wanted: seq[WantedBlob]
    for tx in blk.blobTxs:
      if tx.to == Address.fromHex(BaseInbox).get:
        for i, vh in tx.versionedHashes:
          wanted.add WantedBlob(index: tx.firstBlobIndex + i, versionedHash: vh)
    let blobs = (waitFor beacon.fetchBlobs(101, wanted)).get
    check decodeOpBlob(blobs[0][]).get == basePayloads[0]
    check partial.blobRequests == before
    check wrongNet.blobRequests == 0

    waitFor beacon.closeWait()
    for m in mocks:
      waitFor m.stop()
      waitFor m.closeWait()

  test "Retry-After parsing and url redaction":
    check parseRetryAfter("5") == Opt.some(5.seconds)
    check parseRetryAfter(" 12 ") == Opt.some(12.seconds)
    check parseRetryAfter("Wed, 21 Oct 2015 07:28:00 GMT").isNone
    check parseRetryAfter("99999999") == Opt.some(5.minutes)
    check parseRetryAfter("-1").isNone
    check displayName("https://eth.example.com/v2/SECRETKEY?x=1") == "https://eth.example.com"
    check displayName("http://127.0.0.1:5052") == "http://127.0.0.1:5052"

suite "Beacon /blobs fast scanner":
  test "decodes compact and pretty-printed responses":
    let a = new Blob
    let b = new Blob
    a[0] = 1; b[BYTES_PER_BLOB - 1] = 2
    for body in [
        "{\"execution_optimistic\":false,\"data\":[\"" & a[].to0xHex() & "\",\"" &
          b[].to0xHex() & "\"]}",
        "{\n  \"data\" : [\n    \"" & a[].to0xHex() & "\" ,\n    \"" &
          b[].to0xHex() & "\"\n  ],\n  \"finalized\": true\n}"]:
      let res = scanBlobsResponse(body.toBytes(), 2).get
      check res[0][] == a[] and res[1][] == b[]

  test "empty list and mismatches fall back (error) instead of guessing":
    check scanBlobsResponse("{\"data\":[]}".toBytes(), 0).get.len == 0
    check scanBlobsResponse("{\"data\":[]}".toBytes(), 1).isErr
    check scanBlobsResponse("{\"data\":[\"0x00\"]}".toBytes(), 1).isErr
    check scanBlobsResponse("{\"other\":1}".toBytes(), 0).isErr
    check scanBlobsResponse("{\"data\":[\"0x".toBytes(), 1).isErr
