# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Arbitrum Nitro blob encoding (`util/blobs` in nitro).
##
## Per blob, in this order:
##   1. bytes 1..31 of every field element (4096 x 31 = 126976 bytes)
##   2. the low 6 "spare" bits of byte 0 of every field element, packed
##      LSB-first into a bit stream (4096 x 6 bits = 3072 bytes)
## so one blob carries 130048 bytes. The top 2 bits of byte 0 stay zero to
## keep the field element below the BLS modulus. The per-blob byte streams
## of one transaction are concatenated and decode as a single RLP string.
##
## The first version of this module only handled step 1. That passed the
## synthetic round-trip tests (encoder and decoder shared the mistake) and
## failed on the first real mainnet batch: "rlp: long string out of bounds".
##
## There is no prose spec; the Nitro source is the reference:
##   https://github.com/OffchainLabs/nitro/blob/master/util/blobs/blobs.go
##   (EncodeBlobs / fillBlobBytes / fillBlobBits / DecodeBlobs, spareBlobBits = 6)
## Batches are posted via SequencerInbox.addSequencerL2BatchFromBlobs:
##   https://github.com/OffchainLabs/nitro-contracts/blob/main/src/bridge/SequencerInbox.sol

import ../spec/[eip4844, rlp_lite]

const
  ArbBytesPerFieldElement = 31
  ArbSpareBits = 6
  ArbSpareBytesPerBlob = FIELD_ELEMENTS_PER_BLOB * ArbSpareBits div 8  # 3072
  ArbBytesPerBlob* =
    ArbBytesPerFieldElement * FIELD_ELEMENTS_PER_BLOB + ArbSpareBytesPerBlob

func decodeArbitrumBlobs*(blobs: openArray[BlobRef]): Result[seq[byte], cstring] =
  if blobs.len == 0:
    return err("arbitrum: no blobs")
  # Unpack straight into one buffer, then slice out the RLP payload in place
  # instead of allocating a second copy.
  var packed = newSeq[byte](blobs.len * ArbBytesPerBlob)
  var o = 0
  for b in blobs:
    for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
      copyMem(addr packed[o], unsafeAddr b[][fe * 32 + 1], ArbBytesPerFieldElement)
      o += ArbBytesPerFieldElement
    var
      acc = 0'u16
      accBits = 0
    for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
      acc = acc or (uint16(b[][fe * 32]) shl accBits)
      accBits += ArbSpareBits
      if accBits >= 8:
        packed[o] = byte(acc and 0xff)
        inc o
        acc = acc shr 8
        accBits -= 8
  let (offset, length) = ? rlpStringBounds(packed)
  if offset > 0:
    moveMem(addr packed[0], addr packed[offset], length)
  packed.setLen(length)
  ok packed

func encodeArbitrumBlobs*(data: openArray[byte]): seq[BlobRef] =
  ## Inverse of `decodeArbitrumBlobs`, mirroring nitro's `fillBlobBytes` +
  ## `fillBlobBits`; used by tests and benchmarks.
  let rlp = rlpEncodeBytes(data)
  var i = 0
  while i < rlp.len:
    let blob = new Blob
    for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
      if i >= rlp.len: break
      let n = min(ArbBytesPerFieldElement, rlp.len - i)
      copyMem(addr blob[fe * 32 + 1], unsafeAddr rlp[i], n)
      i += n
    var
      acc = 0'u16
      accBits = 0
    for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
      if accBits < ArbSpareBits:
        if i < rlp.len:
          acc = acc or (uint16(rlp[i]) shl accBits)
          inc i
        accBits += 8   # past the end we shift in zero bits
      blob[fe * 32] = byte(acc and ((1'u16 shl ArbSpareBits) - 1))
      acc = acc shr ArbSpareBits
      accBits -= ArbSpareBits
    result.add blob
