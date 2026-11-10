# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## EIP-4844 primitives shared by every other module: blob/commitment types,
## versioned hash derivation and allocation-free hex decoding.
##
## Spec: https://eips.ethereum.org/EIPS/eip-4844

import
  std/[hashes, typetraits],
  results, nimcrypto/sha2, stew/byteutils

export results

const
  BYTES_PER_FIELD_ELEMENT* = 32
  FIELD_ELEMENTS_PER_BLOB* = 4096
  BYTES_PER_BLOB* = BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB
  BYTES_PER_COMMITMENT* = 48
  VERSIONED_HASH_VERSION_KZG* = 0x01'u8
  BLOB_TX_TYPE* = 3'u8

type
  Blob* = array[BYTES_PER_BLOB, byte]
  BlobRef* = ref Blob
    ## Blobs are 128 KiB; we pass them by reference everywhere so that one
    ## fetched blob can be shared by every feed event and subscriber.

  KzgCommitment* = array[BYTES_PER_COMMITMENT, byte]
  VersionedHash* = distinct array[32, byte]
  Hash32* = distinct array[32, byte]
  Address* = distinct array[20, byte]

func data*(v: VersionedHash | Hash32 | Address): auto {.inline.} =
  distinctBase(v)

template genEqHash(T: typedesc) =
  func `==`*(a, b: T): bool {.borrow.}
  func hash*(v: T): Hash = hash(distinctBase(v))

genEqHash(VersionedHash)
genEqHash(Hash32)
genEqHash(Address)

func `$`*(v: VersionedHash | Hash32 | Address): string =
  distinctBase(v).to0xHex()

func kzgToVersionedHash*(commitment: KzgCommitment): VersionedHash =
  ## `kzg_to_versioned_hash` from EIP-4844.
  var h = sha256.digest(commitment).data
  h[0] = VERSIONED_HASH_VERSION_KZG
  VersionedHash(h)

# Hex -------------------------------------------------------------------------

func makeHexTable(): array[256, int8] =
  for i in 0 .. 255: result[i] = -1
  for c in '0'..'9': result[ord(c)] = int8(ord(c) - ord('0'))
  for c in 'a'..'f': result[ord(c)] = int8(ord(c) - ord('a') + 10)
  for c in 'A'..'F': result[ord(c)] = int8(ord(c) - ord('A') + 10)

const HexTable = makeHexTable()

template hexNibble(c: char | byte): int8 =
  HexTable[ord(c)]

func strip0x[T: char | byte](s: openArray[T]): int {.inline.} =
  if s.len >= 2 and s[0] == T('0') and (s[1] == T('x') or s[1] == T('X')): 2
  else: 0

func decodeHexInto*[T: char | byte](
    dst: var openArray[byte], src: openArray[T]): Result[void, cstring] =
  ## Decode `src` (optionally 0x-prefixed) into `dst`, which must be exactly
  ## the decoded length. No allocation: this is the hot path for blobs, where
  ## the JSON string is 262 KiB per blob. Accepts bytes so blob hex can be
  ## decoded straight out of an HTTP response body.
  let start = strip0x(src)
  if src.len - start != dst.len * 2:
    return err("hex length mismatch")
  var
    i = start
    bad = 0'i8
  for o in 0 ..< dst.len:
    let
      hi = hexNibble(src[i])
      lo = hexNibble(src[i + 1])
    # branch-free validity accumulation: any -1 sets the sign bit
    bad = bad or hi or lo
    dst[o] = byte((hi shl 4) or lo)
    i += 2
  if bad < 0:
    return err("invalid hex character")
  ok()

func decodeHexSeq*(src: openArray[char]): Result[seq[byte], cstring] =
  let start = strip0x(src)
  if (src.len - start) mod 2 != 0:
    return err("odd hex length")
  var res = newSeq[byte]((src.len - start) div 2)
  ? decodeHexInto(res, src)
  ok res

template genFromHex(T: typedesc, N: static int) =
  func fromHex*(_: typedesc[T], s: openArray[char]): Result[T, cstring] =
    var a: array[N, byte]
    ? decodeHexInto(a, s)
    ok T(a)

genFromHex(VersionedHash, 32)
genFromHex(Hash32, 32)
genFromHex(Address, 20)

func fromHex*(_: typedesc[KzgCommitment], s: openArray[char]): Result[KzgCommitment, cstring] =
  var a: KzgCommitment
  ? decodeHexInto(a, s)
  ok a

func blobFromHex*(s: openArray[char]): Result[BlobRef, cstring] =
  let b = new Blob
  ? decodeHexInto(b[], s)
  ok b

func parseQuantity*(s: openArray[char]): Result[uint64, cstring] =
  ## JSON-RPC QUANTITY ("0x1a") or a plain decimal string (beacon API style).
  if s.len == 0:
    return err("empty quantity")
  var v: uint64
  if strip0x(s) == 2:
    if s.len == 2 or s.len > 18:
      return err("invalid hex quantity")
    for i in 2 ..< s.len:
      let n = hexNibble(s[i])
      if n < 0: return err("invalid hex quantity")
      v = (v shl 4) or uint64(n)
  else:
    for c in s:
      if c notin {'0'..'9'}: return err("invalid decimal quantity")
      let d = uint64(ord(c) - ord('0'))
      if v > (high(uint64) - d) div 10: return err("quantity overflow")
      v = v * 10 + d
  ok v

func toQuantity*(v: uint64): string =
  const digits = "0123456789abcdef"
  if v == 0: return "0x0"
  var
    buf: array[16, char]
    n = v
    i = buf.len
  while n != 0:
    dec i
    buf[i] = digits[int(n and 0xf)]
    n = n shr 4
  result = "0x"
  for j in i ..< buf.len: result.add buf[j]
