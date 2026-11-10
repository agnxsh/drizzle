# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Feed events: what subscribers receive. Events are immutable once published
## and shared by reference across all subscribers of a feed.

import ../spec/eip4844

export eip4844

type
  FeedEventKind* {.pure.} = enum
    ## On the wire the kind byte is `ord(kind) + 1` (Batch = 1 ... Heartbeat = 4).
    Batch      ## one L1 blob tx for this L2, decoded
    Reorg      ## discard everything from L1 blocks > rewindTo
    Lagged     ## you were too slow and have been dropped; reconnect
    Heartbeat  ## keep-alive on idle streams

  WireVariant* {.pure.} = enum
    Binary, BinaryWithBlobs, Json, JsonWithBlobs

  FeedEvent* = ref object
    seqNo*: uint64     ## per-feed, starts at 1, assigned by the hub
    feed*: string
    encoded*: array[WireVariant, seq[byte]]
      ## Lazily filled wire encodings, so fan-out to N subscribers encodes
      ## once per variant instead of N times. Only `wire.nim` touches this.
    case kind*: FeedEventKind
    of FeedEventKind.Batch:
      l1Number*: uint64
      l1Hash*: Hash32
      l1Timestamp*: uint64
      slot*: uint64
      txHash*: Hash32
      sender*: Address
      versionedHashes*: seq[VersionedHash]
      blobs*: seq[BlobRef]
        ## May be empty when the relayer runs with `--retain-blobs=false`.
      payloads*: seq[seq[byte]]
      decodeError*: string
        ## Non-empty when the adapter couldn't decode; the event is still
        ## delivered (with raw blobs) so consumers notice instead of
        ## silently missing a batch.
    of FeedEventKind.Reorg:
      rewindTo*: uint64
    of FeedEventKind.Lagged, FeedEventKind.Heartbeat:
      discard

func payloadBytes*(ev: FeedEvent): int =
  if ev.kind == FeedEventKind.Batch:
    for p in ev.payloads: result += p.len
