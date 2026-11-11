# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Stream wire formats.
##
## Binary (default), all integers big-endian:
##
##   frame     := bodyLen:u32 body
##   body      := version:u8(=1) kind:u8 seq:u64 <kind payload>
##   kind      := 1 Batch | 2 Reorg | 3 Lagged | 4 Heartbeat
##   Batch     := l1Number:u64 l1Hash:32 l1Timestamp:u64 slot:u64 txHash:32
##                sender:20 nHashes:u16 versionedHash:32 * nHashes
##                hasBlobs:u8 [blob:131072 * nHashes]
##                nPayloads:u16 (len:u32 bytes) * nPayloads
##                errLen:u16 err:utf8
##   Reorg     := rewindTo:u64
##   Lagged    := (empty)
##   Heartbeat := (empty)
##
## Json: one JSON object per line (ndjson); byte fields are 0x-hex.
## Summary: like Json, but without `payloads` / `blobs`, only their sizes in
## `payloadSizes`. Cheap enough to drive a dashboard.

import
  std/json,
  stew/byteutils,
  ./events

export events

const
  WireVersion* = 1'u8
  FrameHeaderLen* = 4

# Encoding --------------------------------------------------------------------

func putU16(buf: var seq[byte], v: uint16) =
  buf.add byte(v shr 8)
  buf.add byte(v)

func putU32(buf: var seq[byte], v: uint32) =
  buf.add byte(v shr 24)
  buf.add byte(v shr 16)
  buf.add byte(v shr 8)
  buf.add byte(v)

func putU64(buf: var seq[byte], v: uint64) =
  for shift in countdown(56, 0, 8):
    buf.add byte(v shr shift)

func putBytes(buf: var seq[byte], data: openArray[byte]) =
  if data.len > 0:
    let o = buf.len
    buf.setLen(o + data.len)
    copyMem(addr buf[o], unsafeAddr data[0], data.len)

func encodedSizeHint(ev: FeedEvent, withBlobs: bool): int =
  result = 64
  if ev.kind == FeedEventKind.Batch:
    result += 140 + 32 * ev.versionedHashes.len + ev.payloadBytes +
      6 * ev.payloads.len + ev.decodeError.len
    if withBlobs: result += BYTES_PER_BLOB * ev.blobs.len

func encodeBinary(ev: FeedEvent, withBlobs: bool): seq[byte] =
  var buf = newSeqOfCap[byte](encodedSizeHint(ev, withBlobs))
  buf.putU32(0)  # patched below
  buf.add WireVersion
  buf.add byte(ord(ev.kind) + 1)
  buf.putU64(ev.seqNo)
  case ev.kind
  of FeedEventKind.Batch:
    buf.putU64(ev.l1Number)
    buf.putBytes(ev.l1Hash.data)
    buf.putU64(ev.l1Timestamp)
    buf.putU64(ev.slot)
    buf.putBytes(ev.txHash.data)
    buf.putBytes(ev.sender.data)
    buf.putU16(uint16(ev.versionedHashes.len))
    for vh in ev.versionedHashes:
      buf.putBytes(vh.data)
    let includeBlobs = withBlobs and ev.blobs.len == ev.versionedHashes.len
    buf.add(if includeBlobs: 1'u8 else: 0'u8)
    if includeBlobs:
      for b in ev.blobs:
        buf.putBytes(b[])
    buf.putU16(uint16(ev.payloads.len))
    for p in ev.payloads:
      buf.putU32(uint32(p.len))
      buf.putBytes(p)
    let errLen = min(ev.decodeError.len, int(high(uint16)))
    buf.putU16(uint16(errLen))
    buf.putBytes(ev.decodeError.toOpenArrayByte(0, errLen - 1))
  of FeedEventKind.Reorg:
    buf.putU64(ev.rewindTo)
  of FeedEventKind.Lagged, FeedEventKind.Heartbeat:
    discard
  let bodyLen = uint32(buf.len - FrameHeaderLen)
  buf[0] = byte(bodyLen shr 24)
  buf[1] = byte(bodyLen shr 16)
  buf[2] = byte(bodyLen shr 8)
  buf[3] = byte(bodyLen)
  buf

proc toJsonNode*(ev: FeedEvent, withBlobs: bool, withPayloads = true): JsonNode =
  result = %*{"seq": ev.seqNo, "feed": ev.feed, "kind": $ev.kind}
  case ev.kind
  of FeedEventKind.Batch:
    result["l1Number"] = %ev.l1Number
    result["l1Hash"] = %($ev.l1Hash)
    result["l1Timestamp"] = %ev.l1Timestamp
    result["slot"] = %ev.slot
    result["txHash"] = %($ev.txHash)
    result["sender"] = %($ev.sender)
    var hashes = newJArray()
    for vh in ev.versionedHashes: hashes.add %($vh)
    result["versionedHashes"] = hashes
    var sizes = newJArray()
    for p in ev.payloads: sizes.add %p.len
    result["payloadSizes"] = sizes
    if withPayloads:
      var payloads = newJArray()
      for p in ev.payloads: payloads.add %(p.to0xHex())
      result["payloads"] = payloads
    if ev.decodeError.len > 0:
      result["decodeError"] = %ev.decodeError
    if withBlobs and ev.blobs.len > 0:
      var blobs = newJArray()
      for b in ev.blobs: blobs.add %(b[].to0xHex())
      result["blobs"] = blobs
  of FeedEventKind.Reorg:
    result["rewindTo"] = %ev.rewindTo
  of FeedEventKind.Lagged, FeedEventKind.Heartbeat:
    discard

proc encodeJsonLine(ev: FeedEvent, withBlobs, withPayloads: bool): seq[byte] =
  var s = $ev.toJsonNode(withBlobs, withPayloads)
  s.add '\n'
  s.toBytes()

proc encode*(ev: FeedEvent, variant: WireVariant): lent seq[byte] =
  ## Cached: the first subscriber asking for a variant pays for the encode,
  ## everyone after gets the same buffer.
  if ev.encoded[variant].len == 0:
    ev.encoded[variant] =
      case variant
      of WireVariant.Binary: encodeBinary(ev, false)
      of WireVariant.BinaryWithBlobs: encodeBinary(ev, true)
      of WireVariant.Json: encodeJsonLine(ev, false, true)
      of WireVariant.JsonWithBlobs: encodeJsonLine(ev, true, true)
      of WireVariant.JsonSummary: encodeJsonLine(ev, false, false)
  ev.encoded[variant]

func wireVariant*(json, withBlobs: bool): WireVariant =
  if json:
    if withBlobs: WireVariant.JsonWithBlobs else: WireVariant.Json
  else:
    if withBlobs: WireVariant.BinaryWithBlobs else: WireVariant.Binary

# Decoding (reference client + tests) ----------------------------------------

type WireReader = object
  pos: int

template need(r: WireReader, limit: int, n: int) =
  if n < 0 or r.pos + n > limit:
    return err("wire: truncated frame")

func getU16(r: var WireReader, data: openArray[byte]): uint16 =
  result = (uint16(data[r.pos]) shl 8) or uint16(data[r.pos + 1])
  r.pos += 2

func getU32(r: var WireReader, data: openArray[byte]): uint32 =
  for i in 0 .. 3:
    result = (result shl 8) or uint32(data[r.pos + i])
  r.pos += 4

func getU64(r: var WireReader, data: openArray[byte]): uint64 =
  for i in 0 .. 7:
    result = (result shl 8) or uint64(data[r.pos + i])
  r.pos += 8

func getArray[N: static int](r: var WireReader, data: openArray[byte]): array[N, byte] =
  copyMem(addr result[0], unsafeAddr data[r.pos], N)
  r.pos += N

func decodeFrame*(data: openArray[byte]): Result[(FeedEvent, int), string] =
  ## Decode one binary frame from the start of `data`. Returns the event and
  ## the number of bytes consumed.
  var r = WireReader()
  r.need(data.len, FrameHeaderLen)
  let bodyLen = int(r.getU32(data))
  r.need(data.len, bodyLen)
  let frameEnd = FrameHeaderLen + bodyLen

  r.need(frameEnd, 10)
  if data[r.pos] != WireVersion:
    return err("wire: unsupported version")
  inc r.pos
  let kindByte = int(data[r.pos]) - 1
  inc r.pos
  if kindByte < ord(low(FeedEventKind)) or kindByte > ord(high(FeedEventKind)):
    return err("wire: unknown event kind")
  let kind = FeedEventKind(kindByte)
  let seqNo = r.getU64(data)

  var ev: FeedEvent
  case kind
  of FeedEventKind.Batch:
    ev = FeedEvent(kind: FeedEventKind.Batch, seqNo: seqNo)
    r.need(frameEnd, 8 + 32 + 8 + 8 + 32 + 20 + 2)
    ev.l1Number = r.getU64(data)
    ev.l1Hash = Hash32(r.getArray[:32](data))
    ev.l1Timestamp = r.getU64(data)
    ev.slot = r.getU64(data)
    ev.txHash = Hash32(r.getArray[:32](data))
    ev.sender = Address(r.getArray[:20](data))
    let nHashes = int(r.getU16(data))
    r.need(frameEnd, 32 * nHashes + 1)
    for _ in 0 ..< nHashes:
      ev.versionedHashes.add VersionedHash(r.getArray[:32](data))
    let hasBlobs = data[r.pos] == 1
    inc r.pos
    if hasBlobs:
      r.need(frameEnd, BYTES_PER_BLOB * nHashes)
      for _ in 0 ..< nHashes:
        let b = new Blob
        copyMem(addr b[0], unsafeAddr data[r.pos], BYTES_PER_BLOB)
        r.pos += BYTES_PER_BLOB
        ev.blobs.add b
    r.need(frameEnd, 2)
    let nPayloads = int(r.getU16(data))
    for _ in 0 ..< nPayloads:
      r.need(frameEnd, 4)
      let plen = int(r.getU32(data))
      r.need(frameEnd, plen)
      ev.payloads.add @(data.toOpenArray(r.pos, r.pos + plen - 1))
      r.pos += plen
    r.need(frameEnd, 2)
    let errLen = int(r.getU16(data))
    r.need(frameEnd, errLen)
    ev.decodeError = string.fromBytes(data.toOpenArray(r.pos, r.pos + errLen - 1))
    r.pos += errLen
  of FeedEventKind.Reorg:
    ev = FeedEvent(kind: FeedEventKind.Reorg, seqNo: seqNo)
    r.need(frameEnd, 8)
    ev.rewindTo = r.getU64(data)
  of FeedEventKind.Lagged:
    ev = FeedEvent(kind: FeedEventKind.Lagged, seqNo: seqNo)
  of FeedEventKind.Heartbeat:
    ev = FeedEvent(kind: FeedEventKind.Heartbeat, seqNo: seqNo)

  if r.pos != frameEnd:
    return err("wire: trailing bytes in frame")
  ok((ev, frameEnd))
