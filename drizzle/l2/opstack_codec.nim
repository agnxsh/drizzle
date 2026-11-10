# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## OP-stack blob data encoding (version 0), as specified in the OP derivation
## spec and implemented in op-service `eth/blob.go`.
##
## Each round packs 4 field elements = 127 bytes of payload: 31 bytes in the
## low part of each FE, plus 4 x 6 spare bits (the top 2 bits of each FE's
## first byte must stay 0 to remain < BLS modulus) reassembled into 3 bytes.
## Round 0 also carries the version byte and a 3-byte big-endian length.
##
## Spec:
##   https://specs.optimism.io/protocol/ecotone/derivation.html#blob-encoding
##   https://specs.optimism.io/protocol/ecotone/derivation.html#ecotone-blob-retrieval
## Reference implementation (ToData / FromData):
##   https://github.com/ethereum-optimism/optimism/blob/develop/op-service/eth/blob.go
## Batch/frame format carried inside the decoded data:
##   https://specs.optimism.io/protocol/derivation.html

import ../spec/eip4844

const
  OpBlobEncodingVersion* = 0'u8
  OpVersionOffset = 1
  OpRounds = 1024
  OpMaxBlobDataSize* = (4 * 31 + 3) * OpRounds - 4  # 130044

func decodeFieldElement(
    blob: Blob, opos, ipos: var int, output: var seq[byte]
): Result[byte, cstring] {.inline.} =
  if (blob[ipos] and 0b1100_0000'u8) != 0:
    return err("opstack: invalid field element, high bits set")
  # `output` is sized with slack so this copy never needs bounds juggling.
  copyMem(addr output[opos], unsafeAddr blob[ipos + 1], 31)
  let first = blob[ipos]
  opos += 32
  ipos += 32
  ok first

func reassembleBytes(
    opos: var int, encoded: array[4, byte], output: var seq[byte]) {.inline.} =
  dec opos  # there is no 128th output byte per round
  let
    x = (encoded[0] and 0b0011_1111'u8) or ((encoded[1] and 0b0011_0000'u8) shl 2)
    y = (encoded[1] and 0b0000_1111'u8) or ((encoded[3] and 0b0000_1111'u8) shl 4)
    z = (encoded[2] and 0b0011_1111'u8) or ((encoded[3] and 0b0011_0000'u8) shl 2)
  output[opos - 32] = z
  output[opos - 32 * 2] = y
  output[opos - 32 * 3] = x

func decodeOpBlob*(blob: Blob): Result[seq[byte], cstring] =
  if blob[OpVersionOffset] != OpBlobEncodingVersion:
    return err("opstack: unsupported blob encoding version")

  let outputLen =
    (int(blob[2]) shl 16) or (int(blob[3]) shl 8) or int(blob[4])
  if outputLen > OpMaxBlobDataSize:
    return err("opstack: declared length exceeds max blob data size")

  # +32 slack: the final FE copy of the last round may run past the max.
  var output = newSeq[byte](OpMaxBlobDataSize + 32)
  copyMem(addr output[0], unsafeAddr blob[5], 27)

  var
    opos = 28
    ipos = 32
    encoded: array[4, byte]
  encoded[0] = blob[0]
  for i in 1 .. 3:
    encoded[i] = ? decodeFieldElement(blob, opos, ipos, output)
  reassembleBytes(opos, encoded, output)

  var round = 1
  while round < OpRounds and opos < outputLen:
    for j in 0 .. 3:
      encoded[j] = ? decodeFieldElement(blob, opos, ipos, output)
    reassembleBytes(opos, encoded, output)
    inc round

  for i in outputLen ..< OpMaxBlobDataSize:
    if output[i] != 0:
      return err("opstack: non-zero data after declared length")
  for i in ipos ..< BYTES_PER_BLOB:
    if blob[i] != 0:
      return err("opstack: non-zero blob bytes after encoded data")

  output.setLen(outputLen)
  ok output

func encodeOpBlob*(data: openArray[byte]): Result[BlobRef, cstring] =
  ## Inverse of `decodeOpBlob`. Used by tests and benchmarks today, and
  ## the hook for a future write-side (batch posting) mode.
  if data.len > OpMaxBlobDataSize:
    return err("opstack: data too large for one blob")

  let blob = new Blob
  var
    readOffset = 0
    writeOffset = 0
    buf31: array[31, byte]

  template read1(): byte =
    if readOffset >= data.len: 0'u8
    else:
      inc readOffset
      data[readOffset - 1]

  template read31() =
    zeroMem(addr buf31[0], 31)
    if readOffset < data.len:
      let n = min(31, data.len - readOffset)
      copyMem(addr buf31[0], unsafeAddr data[readOffset], n)
      readOffset += n

  template write1(v: byte) =
    blob[writeOffset] = v
    inc writeOffset

  template write31() =
    copyMem(addr blob[writeOffset], addr buf31[0], 31)
    writeOffset += 31

  var round = 0
  while round < OpRounds and readOffset < data.len:
    if round == 0:
      zeroMem(addr buf31[0], 31)
      buf31[0] = OpBlobEncodingVersion
      let ilen = uint32(data.len)
      buf31[1] = byte(ilen shr 16)
      buf31[2] = byte(ilen shr 8)
      buf31[3] = byte(ilen)
      let n = min(27, data.len)
      if n > 0:
        copyMem(addr buf31[4], unsafeAddr data[0], n)
      readOffset += n
    else:
      read31()

    let x = read1()
    write1(x and 0b0011_1111'u8)
    write31()

    read31()
    let y = read1()
    write1((y and 0b0000_1111'u8) or ((x and 0b1100_0000'u8) shr 2))
    write31()

    read31()
    let z = read1()
    write1(z and 0b0011_1111'u8)
    write31()

    read31()
    write1(((z and 0b1100_0000'u8) shr 2) or ((y and 0b1111_0000'u8) shr 4))
    write31()
    inc round

  if readOffset < data.len:
    return err("opstack: failed to fit data into blob")
  ok blob
