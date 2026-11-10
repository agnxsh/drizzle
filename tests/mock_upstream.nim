# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## In-process fake of an EL JSON-RPC endpoint + beacon REST API, serving a
## scripted chain. Used by the end-to-end relayer tests.

import
  std/[json, strutils, tables],
  chronos, chronos/apps/http/httpserver, stew/byteutils, nimcrypto/sha2,
  ../drizzle/spec/eip4844

export json, tables, httpserver

const
  MockGenesis* = 1_600_000_000'u64
  MockSlotSecs* = 12'u64

type
  MockBlob = object
    commitment: KzgCommitment
    blob: BlobRef

  MockTx* = object
    sender*, to*: string
    blobs*: seq[BlobRef]

  MockChain* = ref object
    head*: uint64
    blocks: Table[uint64, JsonNode]
    slotBlobs: Table[uint64, seq[VersionedHash]]  # slot -> blobs in block order
    blobs: Table[VersionedHash, MockBlob]
    noBlobsEndpoint*: bool     ## pretend to be a pre-Fulu beacon node
    blobsRequests*, sidecarRequests*: int
    nonce: int
    latency*: Duration         ## simulated network round trip per request
    cacheResponses*: bool      ## benchmarks: don't re-serialize blob JSON
    responseCache: Table[string, string]

func fakeHash*(number: uint64, variant: int): Hash32 =
  var h = sha256.digest($number & "/" & $variant).data
  Hash32(h)

func tx0(): string = "0x" & "00".repeat(20)

proc commitFor(c: MockChain): KzgCommitment =
  inc c.nonce
  let d = sha256.digest("commitment" & $c.nonce).data
  result[0] = 0xb0
  for i in 0 ..< 32: result[1 + i] = d[i]

proc setBlock*(
    c: MockChain, number: uint64, variant: int, parentVariant: int,
    txs: openArray[MockTx]) =
  ## Insert/replace block `number`. Hashes are derived from (number, variant)
  ## so a "reorged" block is just the same number with another variant.
  let slot = number  # timestamp = genesis + 12 * number
  var
    jtxs = newJArray()
    order: seq[VersionedHash]
  for i, tx in txs:
    var hashes = newJArray()
    for b in tx.blobs:
      let commitment = c.commitFor()
      let vh = kzgToVersionedHash(commitment)
      c.blobs[vh] = MockBlob(commitment: commitment, blob: b)
      order.add vh
      hashes.add %($vh)
    jtxs.add %*{
      "type": "0x3", "hash": $fakeHash(number * 1000 + uint64(i), variant),
      "from": tx.sender, "to": tx.to, "blobVersionedHashes": hashes}
  # a non-blob tx too, to make sure it's skipped
  jtxs.add %*{"type": "0x2", "hash": $fakeHash(number, 99),
              "from": tx0(), "to": tx0()}
  c.blocks[number] = %*{
    "number": toQuantity(number),
    "timestamp": toQuantity(MockGenesis + MockSlotSecs * slot),
    "hash": $fakeHash(number, variant),
    "parentHash": $fakeHash(number - 1, parentVariant),
    "transactions": jtxs}
  c.slotBlobs[slot] = order

proc rpc(c: MockChain, body: string): JsonNode =
  let req =
    try: parseJson(body)
    except CatchableError: return %*{"jsonrpc": "2.0", "id": 0, "error": "bad json"}
  let id = req{"id"}
  case req{"method"}.getStr
  of "eth_blockNumber":
    %*{"jsonrpc": "2.0", "id": id, "result": toQuantity(c.head)}
  of "eth_getBlockByNumber":
    let n = parseQuantity(req{"params"}[0].getStr).get(high(uint64))
    let blk =
      if n <= c.head and n in c.blocks: c.blocks.getOrDefault(n)
      else: newJNull()
    %*{"jsonrpc": "2.0", "id": id, "result": blk}
  else:
    %*{"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "nope"}}

proc beacon(c: MockChain, req: HttpRequestRef, parts: seq[string]): (HttpCode, JsonNode) =
  let path = parts.join("/")
  if path == "eth/v1/beacon/genesis":
    return (Http200, %*{"data": {"genesis_time": $MockGenesis}})
  if path == "eth/v1/config/spec":
    return (Http200, %*{"data": {"SECONDS_PER_SLOT": $MockSlotSecs}})
  if parts.len == 5 and parts[3] in ["blobs", "blob_sidecars"]:
    let slot =
      try: parseBiggestUInt(parts[4]).uint64
      except ValueError: return (Http400, %*{"message": "bad slot"})
    if slot notin c.slotBlobs:
      return (Http404, %*{"message": "block not found"})
    let order = c.slotBlobs.getOrDefault(slot)
    if parts[3] == "blobs":
      inc c.blobsRequests
      if c.noBlobsEndpoint:
        return (Http404, %*{"message": "endpoint not found"})
      var data = newJArray()
      for s in req.query.getList("versioned_hashes"):
        let vh = VersionedHash.fromHex(s).valueOr:
          return (Http400, %*{"message": "bad hash"})
        data.add %(c.blobs.getOrDefault(vh).blob[].to0xHex())
      return (Http200, %*{"data": data})
    else:
      inc c.sidecarRequests
      var data = newJArray()
      for s in req.query.getList("indices"):
        let i = try: parseInt(s) except ValueError: -1
        if i < 0 or i >= order.len:
          return (Http400, %*{"message": "bad index"})
        let b = c.blobs.getOrDefault(order[i])
        data.add %*{"index": $i, "blob": b.blob[].to0xHex(),
                    "kzg_commitment": b.commitment.to0xHex()}
      return (Http200, %*{"data": data})
  (Http404, %*{"message": "not found"})

proc prewarmBlobResponses*(c: MockChain) =
  ## Benchmarks: pre-serialize the `/blobs` response for "all blobs of the
  ## slot", keyed exactly like the relayer's request, so the mock spends no
  ## CPU on the shared core during measurement.
  for slot, order in c.slotBlobs:
    if order.len == 0: continue
    var query = ""
    var data = newJArray()
    for i, vh in order:
      if i > 0: query.add "&"
      query.add "versioned_hashes=" & $vh
      data.add %(c.blobs.getOrDefault(vh).blob[].to0xHex())
    c.responseCache["/eth/v1/beacon/blobs/" & $slot & "?" & query] =
      $(%*{"data": data})

proc startMock*(c: MockChain): (HttpServerRef, string) =
  proc process(r: RequestFence): Future[HttpResponseRef] {.
      async: (raises: [CancelledError]).} =
    let req = r.valueOr:
      return defaultResponse()
    let headers = HttpTable.init([("Content-Type", "application/json")])
    try:
      if c.latency > ZeroDuration:
        await sleepAsync(c.latency)
      if c.cacheResponses and req.meth == MethodGet:
        let key = req.uri.path & "?" & req.uri.query
        if key in c.responseCache:
          return await req.respond(Http200, c.responseCache.getOrDefault(key), headers)
        let (code, node) = c.beacon(req, req.uri.path.strip(chars = {'/'}).split('/'))
        let body = $node
        if code == Http200: c.responseCache[key] = body
        return await req.respond(code, body, headers)
      if req.meth == MethodPost:
        let body = string.fromBytes(await req.getBody())
        return await req.respond(Http200, $c.rpc(body), headers)
      let parts = req.uri.path.strip(chars = {'/'}).split('/')
      let (code, node) = c.beacon(req, parts)
      await req.respond(code, $node, headers)
    except CancelledError as exc:
      raise exc
    except CatchableError:
      defaultResponse()

  let address =
    try: initTAddress("127.0.0.1:0")
    except TransportAddressError as exc: raiseAssert exc.msg
  let server = HttpServerRef.new(
    address, process,
    maxRequestBodySize = 1 shl 20).expect("mock server")
  server.start()
  (server, "http://" & $server.instance.localAddress())
