# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.used.}

import
  std/[json, strutils],
  unittest2,
  ../drizzle/l2/presets,
  ../drizzle/rpc/el_client

const
  Inbox = "0xff00000000000000000000000000000000008453"
  Batcher = "0x5050f69a9786f081509234f1a7f4684b5e5b76c9"
  Other = "0x1111111111111111111111111111111111111111"

func toAddr(s: string): Address = Address.fromHex(s).get

suite "Feed specs and adapter index":
  test "presets":
    let a = parseFeedSpec("base").get
    check:
      a.name == "base"
      a.kind == "opstack"
      a.inbox == toAddr(Inbox)
      a.batchers == @[toAddr(Batcher)]
      parseFeedSpec("arbitrum-one").get.kind == "arbitrum"
      parseFeedSpec("nope").isErr

  test "custom specs":
    let a = parseFeedSpec("mine:raw:" & Other).get
    check a.kind == "raw" and a.batchers.len == 0
    let b = parseFeedSpec("orbit:arbitrum:" & Other & ":" & Batcher & "," & Inbox).get
    check b.batchers.len == 2
    check:
      parseFeedSpec("x:unknownkind:" & Other).isErr
      parseFeedSpec("x:raw:0x12").isErr
      parseFeedSpec("bad name:raw:" & Other).isErr
      parseFeedSpec("a:b").isErr

  test "index matches on inbox and batcher allow-list":
    let
      base = parseFeedSpec("base").get
      anyone = parseFeedSpec("basetap:raw:" & Inbox).get
      idx = AdapterIndex.init([base, anyone]).get
    var tx = BlobTx(to: toAddr(Inbox), sender: toAddr(Batcher))
    check idx.matching(tx).len == 2
    tx.sender = toAddr(Other)
    check idx.matching(tx).len == 1   # only the open raw feed
    tx.to = toAddr(Other)
    check idx.matching(tx).len == 0
    check AdapterIndex.init([base, base]).isErr

  test "registry accepts new adapter kinds":
    registerAdapterKind("echo", proc(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string] {.noSideEffect.} =
      ok @[@[byte(blobs.len)]])
    let a = newAdapter("e", "echo", toAddr(Other)).get
    check a.decode([new Blob, new Blob]).get == @[@[2'u8]]

suite "EL block parsing":
  test "extracts type-3 txs with block-level blob indices":
    let vh = "0x01" & "00".repeat(31)
    let blk = %*{
      "number": "0x64", "timestamp": "0x5fc63057",
      "hash": "0x" & "aa".repeat(32), "parentHash": "0x" & "bb".repeat(32),
      "transactions": [
        {"type": "0x2", "hash": "0x" & "01".repeat(32), "from": Other, "to": Other},
        {"type": "0x3", "hash": "0x" & "02".repeat(32), "from": Other, "to": Other,
         "blobVersionedHashes": [vh, vh]},
        {"type": "0x3", "hash": "0x" & "03".repeat(32), "from": Batcher, "to": Inbox,
         "blobVersionedHashes": [vh, vh, vh]},
      ]}
    let b = parseBlock(blk).get
    check:
      b.number == 100
      b.blobTxs.len == 2
      b.blobCount == 5
      b.blobTxs[0].firstBlobIndex == 0
      b.blobTxs[1].firstBlobIndex == 2
      b.blobTxs[1].to == toAddr(Inbox)
      b.blobTxs[1].versionedHashes.len == 3

  test "rejects malformed blob txs":
    let blk = %*{
      "number": "0x1", "timestamp": "0x1",
      "hash": "0x" & "aa".repeat(32), "parentHash": "0x" & "bb".repeat(32),
      "transactions": [{"type": "0x3", "hash": "0x" & "02".repeat(32),
                        "from": Other, "to": Other}]}
    check parseBlock(blk).isErr
