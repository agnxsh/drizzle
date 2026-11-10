# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Execution-layer JSON-RPC: head tracking and block -> blob tx extraction.

import
  ./json_http, ../l2/adapter

export json_http

type
  ElBlock* = object
    number*: uint64
    hash*, parentHash*: Hash32
    timestamp*: uint64
    blobTxs*: seq[BlobTx]   ## every type-3 tx, in block order
    blobCount*: int         ## total blobs in the block

  ElClient* = ref object
    http*: JsonHttpClient

func jstr(n: JsonNode, key: string): string =
  let v = n{key}
  if v.isNil or v.kind != JString: "" else: v.str

proc parseBlobTx(tx: JsonNode, firstBlobIndex: int): Result[BlobTx, string] =
  var res = BlobTx(firstBlobIndex: firstBlobIndex)
  res.hash = Hash32.fromHex(tx.jstr("hash")).valueOr:
    return err("bad tx hash")
  res.sender = Address.fromHex(tx.jstr("from")).valueOr:
    return err("bad tx sender")
  # Type-3 txs can't be contract creations, so `to` is always set.
  res.to = Address.fromHex(tx.jstr("to")).valueOr:
    return err("bad tx to")
  let hashes = tx{"blobVersionedHashes"}
  if hashes.isNil or hashes.kind != JArray or hashes.len == 0:
    return err("blob tx without blobVersionedHashes")
  for h in hashes:
    if h.kind != JString: return err("bad versioned hash")
    let vh = VersionedHash.fromHex(h.str).valueOr:
      return err("bad versioned hash")
    res.versionedHashes.add vh
  ok res

proc parseBlock*(n: JsonNode): Result[ElBlock, string] =
  ## Exposed for tests.
  if n.kind != JObject:
    return err("block is not an object")
  var res: ElBlock
  res.number = parseQuantity(n.jstr("number")).valueOr:
    return err("bad block number")
  res.timestamp = parseQuantity(n.jstr("timestamp")).valueOr:
    return err("bad block timestamp")
  res.hash = Hash32.fromHex(n.jstr("hash")).valueOr:
    return err("bad block hash")
  res.parentHash = Hash32.fromHex(n.jstr("parentHash")).valueOr:
    return err("bad parent hash")
  let txs = n{"transactions"}
  if txs.isNil or txs.kind != JArray:
    return err("missing transactions")
  for tx in txs:
    if tx.kind != JObject:
      return err("transactions must be full objects")
    let txType = parseQuantity(tx.jstr("type")).valueOr:
      continue
    if txType != BLOB_TX_TYPE:
      continue
    let btx = ? parseBlobTx(tx, res.blobCount)
    res.blobCount += btx.versionedHashes.len
    res.blobTxs.add btx
  ok res

proc new*(T: type ElClient, url: string, timeout: Duration, maxConnections = -1): T =
  T(http: JsonHttpClient.new(url, timeout, maxConnections))

proc blockNumber*(
    c: ElClient
): Future[Result[uint64, string]] {.async: (raises: [CancelledError]).} =
  let n = (await c.http.rpcCall("eth_blockNumber", newJArray())).valueOr:
    return err($error)
  if n.kind != JString:
    return err("eth_blockNumber: not a string")
  parseQuantity(n.str).mapErr(proc(e: cstring): string = $e)

proc getBlock*(
    c: ElClient, number: uint64
): Future[Result[Opt[ElBlock], string]] {.async: (raises: [CancelledError]).} =
  ## `Opt.none` when the node doesn't have the block (yet).
  let n = (await c.http.rpcCall(
      "eth_getBlockByNumber", %*[toQuantity(number), true])).valueOr:
    return err($error)
  if n.kind == JNull:
    return ok(Opt.none(ElBlock))
  let blk = ? parseBlock(n)
  if blk.number != number:
    return err("node returned block " & $blk.number & " for " & $number)
  ok(Opt.some(blk))
