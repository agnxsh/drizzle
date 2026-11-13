# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## The L2 plug-in interface.
##
## An adapter answers two questions for its L2:
##   1. does this L1 blob transaction belong to me? (`inbox` + `batchers`)
##   2. how do I turn its raw blobs into batch payload bytes? (`decode`)
##
## New L2 families plug in by calling `registerAdapterKind` (typically from a
## module-level statement in `drizzle/l2/<family>.nim`) and importing that
## module from `drizzle/l2/builtin.nim` (or any module linked into the binary).

import
  std/[tables, strutils],
  ../spec/eip4844

export eip4844

type
  BlobTx* = object
    ## The parts of an L1 type-3 transaction an adapter cares about.
    hash*: Hash32
    sender*: Address
    to*: Address
    versionedHashes*: seq[VersionedHash]
    firstBlobIndex*: int
      ## Index of this tx's first blob within the L1 block (blob indices are
      ## assigned in tx order across all type-3 txs of the block).

  DecodeProc* = proc(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string] {.
    gcsafe, noSideEffect, raises: [].}
    ## Turn the blobs of one transaction (in tx order) into zero or more
    ## payloads. Must be pure: it runs on the hot path and may be moved to a
    ## worker thread pool later.

  AdapterKind* = object
    decode*: DecodeProc
    requireSuccess*: bool
      ## Only successful txs carry batches. True when the L2 derives batches
      ## from inbox contract events (Arbitrum's SequencerBatchDelivered), so
      ## a reverted tx's blobs are not a batch. False for OP-stack: op-node
      ## reads batcher txs to an EOA inbox without looking at receipts.

  L2Adapter* = ref object
    name*: string            ## feed name, unique, e.g. "base"
    kind*: string            ## adapter kind, e.g. "opstack"
    inbox*: Address          ## L1 `to` address of batch transactions
    batchers*: seq[Address]  ## allowed senders; empty = accept any sender
    decode*: DecodeProc
    requireSuccess*: bool    ## see `AdapterKind.requireSuccess`

  AdapterIndex* = object
    ## `to` address -> adapters, so that per-tx filtering is a single lookup.
    byInbox: Table[Address, seq[L2Adapter]]
    all*: seq[L2Adapter]

var adapterKinds: Table[string, AdapterKind]
  ## Written at module-init time only, read during startup. Never touched
  ## from async code, so no gcsafe concerns in practice.

proc registerAdapterKind*(kind: string, decode: DecodeProc, requireSuccess = false) =
  adapterKinds[kind.toLowerAscii] = AdapterKind(decode: decode, requireSuccess: requireSuccess)

proc registeredKinds*(): seq[string] =
  for k in adapterKinds.keys: result.add k

proc newAdapter*(
    name, kind: string, inbox: Address, batchers: seq[Address] = @[]
): Result[L2Adapter, string] =
  let k = adapterKinds.getOrDefault(kind.toLowerAscii)
  if k.decode.isNil:
    return err("unknown adapter kind '" & kind & "', known: " &
               registeredKinds().join(", "))
  if name.len == 0 or not name.allCharsInSet(IdentChars + {'-'}):
    return err("invalid feed name '" & name & "'")
  ok L2Adapter(
    name: name, kind: kind.toLowerAscii, inbox: inbox, batchers: batchers,
    decode: k.decode, requireSuccess: k.requireSuccess)

func accepts*(a: L2Adapter, tx: BlobTx): bool =
  a.inbox == tx.to and (a.batchers.len == 0 or tx.sender in a.batchers)

func init*(T: type AdapterIndex, adapters: openArray[L2Adapter]): Result[T, string] =
  var res = T()
  for a in adapters:
    for existing in res.all:
      if existing.name == a.name:
        return err("duplicate feed name '" & a.name & "'")
    res.all.add a
    res.byInbox.mgetOrPut(a.inbox, @[]).add a
  ok res

func matching*(idx: AdapterIndex, tx: BlobTx): seq[L2Adapter] =
  idx.byInbox.withValue(tx.to, adapters):
    for a in adapters:
      if a.accepts(tx):
        result.add a

func hasInbox*(idx: AdapterIndex, to: Address): bool =
  to in idx.byInbox
