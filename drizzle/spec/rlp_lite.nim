# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Just enough RLP to read and write a single byte string, which is all the
## Arbitrum blob encoding needs. Trailing bytes after the item are ignored,
## matching go-ethereum's `rlp.Decode` on a stream.
##
## Spec: https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/

import results

export results

func rlpStringBounds*(data: openArray[byte]): Result[(int, int), cstring] =
  ## Returns `(offset, length)` of the payload of the RLP string at the start
  ## of `data`.
  if data.len == 0:
    return err("rlp: empty input")
  let prefix = int(data[0])
  if prefix < 0x80:
    ok((0, 1))
  elif prefix <= 0xb7:
    let length = prefix - 0x80
    if 1 + length > data.len:
      return err("rlp: short string out of bounds")
    if length == 1 and data[1] < 0x80:
      return err("rlp: non-canonical single byte")
    ok((1, length))
  elif prefix <= 0xbf:
    let lenOfLen = prefix - 0xb7
    if 1 + lenOfLen > data.len:
      return err("rlp: length-of-length out of bounds")
    if data[1] == 0:
      return err("rlp: leading zero in length")
    var length = 0
    for i in 1 .. lenOfLen:
      if length > (high(int) shr 8):
        return err("rlp: length overflow")
      length = (length shl 8) or int(data[i])
    if length < 56:
      return err("rlp: non-canonical long string")
    if length > data.len - 1 - lenOfLen:
      return err("rlp: long string out of bounds")
    ok((1 + lenOfLen, length))
  else:
    err("rlp: expected string, got list")

func rlpDecodeBytes*(data: openArray[byte]): Result[seq[byte], cstring] =
  let (offset, length) = ? rlpStringBounds(data)
  ok @(data.toOpenArray(offset, offset + length - 1))

func rlpEncodeBytes*(data: openArray[byte]): seq[byte] =
  if data.len == 1 and data[0] < 0x80:
    return @[data[0]]
  if data.len < 56:
    result = newSeqOfCap[byte](1 + data.len)
    result.add byte(0x80 + data.len)
  else:
    var
      lenBytes: array[8, byte]
      n = data.len
      k = 0
    while n > 0:
      lenBytes[7 - k] = byte(n and 0xff)
      n = n shr 8
      inc k
    result = newSeqOfCap[byte](1 + k + data.len)
    result.add byte(0xb7 + k)
    for i in 8 - k ..< 8:
      result.add lenBytes[i]
  result.add data
