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
  mock: HttpServerRef
  hub: FeedHub
  relayer: Relayer
  api: ApiServer
  el: ElClient
  beacon: BeaconClient
  apiUrl: string

proc setup(chain: MockChain, startBlock: uint64): Harness =
  let (mock, url) = startMock(chain)
  let adapters = @[
    parseFeedSpec("base").get,
    parseFeedSpec("arbitrum-one").get,
    parseFeedSpec("basetap:raw:" & BaseInbox).get]
  let
    index = AdapterIndex.init(adapters).get
    hub = FeedHub.new(adapters, ringSize = 64, queueSize = 64)
    el = ElClient.new(url, 5.seconds)
    beacon = BeaconClient.new(url, 5.seconds, BeaconApiMode.Auto)
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
  Harness(chain: chain, mock: mock, hub: hub, relayer: relayer, api: api,
          el: el, beacon: beacon, apiUrl: "http://" & $api.localAddress)

proc teardown(h: Harness) =
  waitFor h.relayer.stop()
  waitFor h.api.stop()
  waitFor h.el.http.closeWait()
  waitFor h.beacon.http.closeWait()
  waitFor h.mock.stop()
  waitFor h.mock.closeWait()

proc buildChain(): (MockChain, seq[seq[byte]], seq[byte]) =
  let chain = MockChain()
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
