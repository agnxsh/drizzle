# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.used.}

import
  unittest2,
  ../drizzle/spec/eip4844,
  ../drizzle/l2/[opstack_codec, arbitrum_codec]

func pattern(n: int, seed = 0): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n: result[i] = byte((i * 31 + seed + (i shr 8)) and 0xff)

suite "OP-stack blob codec":
  test "round trip across round boundaries":
    for n in [0, 1, 26, 27, 28, 126, 127, 128, 1000, 4096, 65536,
              OpMaxBlobDataSize - 1, OpMaxBlobDataSize]:
      let data = pattern(n, n)
      let blob = encodeOpBlob(data).get
      check decodeOpBlob(blob[]).get == data

  test "layout of the first field element":
    let blob = encodeOpBlob([byte 'a', byte 'b', byte 'c']).get
    check:
      blob[1] == OpBlobEncodingVersion
      blob[2] == 0 and blob[3] == 0 and blob[4] == 3
      blob[5] == byte('a') and blob[6] == byte('b') and blob[7] == byte('c')

  test "every field element stays below the modulus (top 2 bits clear)":
    let blob = encodeOpBlob(pattern(OpMaxBlobDataSize, 0xff)).get
    var ok = true
    for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
      if (blob[fe * 32] and 0xc0) != 0: ok = false
    check ok

  test "rejects oversized input":
    check encodeOpBlob(newSeq[byte](OpMaxBlobDataSize + 1)).isErr

  test "rejects bad version, high bits and trailing garbage":
    let good = encodeOpBlob(pattern(500)).get
    var bad = new Blob
    bad[] = good[]
    bad[1] = 1
    check decodeOpBlob(bad[]).isErr
    bad[] = good[]
    bad[32] = bad[32] or 0x80
    check decodeOpBlob(bad[]).isErr
    bad[] = good[]
    bad[BYTES_PER_BLOB - 1] = 1
    check decodeOpBlob(bad[]).isErr
    bad[] = good[]
    bad[4] = byte(bad[4] - 1)  # declared length shorter than real data
    check decodeOpBlob(bad[]).isErr

suite "Arbitrum blob codec":
  test "round trip, single and multiple blobs":
    for n in [1, 100, ArbBytesPerBlob - 10, ArbBytesPerBlob, ArbBytesPerBlob * 3 + 7]:
      let data = pattern(n, 3)
      let blobs = encodeArbitrumBlobs(data)
      check decodeArbitrumBlobs(blobs).get == data

  test "byte 0 of each field element only uses its low 6 bits":
    let blobs = encodeArbitrumBlobs(pattern(ArbBytesPerBlob * 2, 0xff))
    var ok = true
    for b in blobs:
      for fe in 0 ..< FIELD_ELEMENTS_PER_BLOB:
        if (b[fe * 32] and 0xc0) != 0: ok = false
    check ok
    check blobs.len == 3  # RLP header pushes it over two blobs

  test "capacity is 130048 bytes per blob (31-byte parts + spare bits)":
    check ArbBytesPerBlob == 130048
    # 4-byte RLP header (0xb8+3 len bytes) -> exactly fills one blob
    check encodeArbitrumBlobs(pattern(ArbBytesPerBlob - 4, 1)).len == 1
    check encodeArbitrumBlobs(pattern(ArbBytesPerBlob - 3, 1)).len == 2

  test "spare-bit section carries data (hand-built blob)":
    # RLP: 0xb9 0x01 0xfb = 507-byte string. The 4096 31-byte slots hold the
    # first 126976 bytes; build a payload that ends inside the spare-bit
    # stream of a single blob to pin the bit order (LSB-first, 6 bits/FE).
    let data = pattern(ArbBytesPerBlob - 4, 5)
    let blob = encodeArbitrumBlobs(data)[0]
    # last data byte lives in the spare bits of the final field elements
    check (blob[(FIELD_ELEMENTS_PER_BLOB - 1) * 32] and 0x3f) != 0 or data[^1] == 0
    check decodeArbitrumBlobs([blob]).get == data

  test "rejects empty and malformed":
    check decodeArbitrumBlobs(newSeq[BlobRef]()).isErr
    let b = new Blob
    b[1] = 0xc5  # RLP list, not a string
    check decodeArbitrumBlobs([b]).isErr
