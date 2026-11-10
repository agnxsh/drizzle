# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.used.}

import
  unittest2, stew/byteutils,
  ../drizzle/spec/[eip4844, rlp_lite]

suite "EIP-4844 primitives":
  test "kzg_to_versioned_hash of the empty-blob commitment":
    var c: KzgCommitment
    c[0] = 0xc0
    check $kzgToVersionedHash(c) ==
      "0x010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014"

  test "hex decoding":
    check:
      Address.fromHex("0xff00000000000000000000000000000000008453").isOk
      Address.fromHex("ff00000000000000000000000000000000008453").isOk
      Address.fromHex("0xff0000000000000000000000000000000000845").isErr
      Address.fromHex("0xzz00000000000000000000000000000000008453").isErr
      decodeHexSeq("0x").get.len == 0
      decodeHexSeq("0xABcd").get == @[0xab'u8, 0xcd]
      decodeHexSeq("0xabc").isErr

  test "blob hex round trip":
    let b = new Blob
    for i in 0 ..< BYTES_PER_BLOB: b[i] = byte(i mod 251)
    let back = blobFromHex(b[].to0xHex()).get
    check back[] == b[]
    check blobFromHex("0x00").isErr

  test "quantities":
    check:
      parseQuantity("0x0").get == 0
      parseQuantity("0x1a").get == 26
      parseQuantity("1606824023").get == 1606824023'u64
      parseQuantity("0xffffffffffffffff").get == high(uint64)
      parseQuantity("0x").isErr
      parseQuantity("18446744073709551616").isErr
      parseQuantity("12a").isErr
      toQuantity(0) == "0x0"
      toQuantity(26) == "0x1a"
      toQuantity(high(uint64)) == "0xffffffffffffffff"

suite "RLP byte strings":
  test "round trip across length classes":
    for n in [0, 1, 2, 55, 56, 255, 256, 65535, 65536, 200_000]:
      var data = newSeq[byte](n)
      for i in 0 ..< n: data[i] = byte((i * 7 + 0x81) mod 256)
      let enc = rlpEncodeBytes(data)
      check rlpDecodeBytes(enc).get == data

  test "single bytes":
    check:
      rlpEncodeBytes([0x05'u8]) == @[0x05'u8]
      rlpEncodeBytes([0x80'u8]) == @[0x81'u8, 0x80]
      rlpDecodeBytes([0x05'u8]).get == @[0x05'u8]

  test "trailing bytes are ignored":
    check rlpDecodeBytes([0x82'u8, 1, 2, 0, 0, 0]).get == @[1'u8, 2]

  test "malformed input":
    check:
      rlpDecodeBytes([]).isErr
      rlpDecodeBytes([0x83'u8, 1, 2]).isErr          # short
      rlpDecodeBytes([0x81'u8, 0x05]).isErr          # non-canonical
      rlpDecodeBytes([0xb8'u8, 10, 1]).isErr         # non-canonical long
      rlpDecodeBytes([0xb9'u8, 0, 60]).isErr         # leading zero
      rlpDecodeBytes([0xc0'u8]).isErr                # list
