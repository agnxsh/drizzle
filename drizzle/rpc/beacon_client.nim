# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Beacon REST API: genesis/spec for timestamp -> slot, and blob retrieval
## via `/eth/v1/beacon/blobs` (post-Fulu) or `/eth/v1/beacon/blob_sidecars`.
## See NOTES.md section 6 for the ordering trick and verification caveats.
##
## Spec: https://ethereum.github.io/beacon-APIs/
##   (getBlobs: /eth/v1/beacon/blobs, getBlobSidecars: /eth/v1/beacon/blob_sidecars)

import
  std/[algorithm, strutils],
  chronicles,
  ./json_http, ../spec/eip4844

export json_http

type
  BeaconApiMode* {.pure.} = enum
    Auto, Blobs, Sidecars

  WantedBlob* = object
    index*: int                  ## blob index within the beacon block
    versionedHash*: VersionedHash

  BeaconClient* = ref object
    http*: JsonHttpClient
    mode*: BeaconApiMode
    resolved: BeaconApiMode      ## Auto until a probe succeeds
    genesisTime*: uint64
    secondsPerSlot*: uint64

func parseBeaconApiMode*(s: string): Result[BeaconApiMode, string] =
  case s.toLowerAscii
  of "auto": ok BeaconApiMode.Auto
  of "blobs": ok BeaconApiMode.Blobs
  of "sidecars": ok BeaconApiMode.Sidecars
  else: err("beacon api mode must be auto, blobs or sidecars")

proc new*(
    T: type BeaconClient, url: string, timeout: Duration,
    mode = BeaconApiMode.Auto, maxConnections = -1): T =
  T(http: JsonHttpClient.new(url, timeout, maxConnections, bufferSize = 1024 * 1024),
    mode: mode, resolved: mode)

func jdata(n: JsonNode): JsonNode =
  if n.kind == JObject: n{"data"} else: nil

func jstr(n: JsonNode, key: string): string =
  if n.isNil or n.kind != JObject: return ""
  let v = n{key}
  if v.isNil or v.kind != JString: "" else: v.str

proc init*(
    c: BeaconClient
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let genesis = (await c.http.getJson("/eth/v1/beacon/genesis")).valueOr:
    return err("genesis: " & $error)
  c.genesisTime = parseQuantity(genesis.jdata.jstr("genesis_time")).valueOr:
    return err("genesis: bad genesis_time")
  let spec = (await c.http.getJson("/eth/v1/config/spec")).valueOr:
    return err("spec: " & $error)
  c.secondsPerSlot = parseQuantity(spec.jdata.jstr("SECONDS_PER_SLOT")).valueOr:
    return err("spec: bad SECONDS_PER_SLOT")
  if c.secondsPerSlot == 0:
    return err("spec: SECONDS_PER_SLOT is 0")
  ok()

func slotAt*(c: BeaconClient, timestamp: uint64): Result[uint64, string] =
  if timestamp < c.genesisTime:
    return err("timestamp before genesis")
  let delta = timestamp - c.genesisTime
  if delta mod c.secondsPerSlot != 0:
    return err("timestamp not aligned to slot boundary")
  ok delta div c.secondsPerSlot

func endpointMissing(f: HttpFailure): bool =
  f.kind == HttpFailureKind.Status and f.status in [400, 404, 405, 501]

func scanBlobsResponse*(
    body: openArray[byte], expected: int): Result[seq[BlobRef], cstring] =
  ## Fast path for `{..., "data": ["0x...", ...]}`: find the `data` array and
  ## hex-decode each string straight from the response bytes into a blob.
  ## Skips building a std/json tree, which would allocate and copy a
  ## 262 KiB string node per blob. Any surprise -> error, caller falls back.
  const key = "\"data\""
  var i = 0
  block search:
    while i + key.len <= body.len:
      var matched = true
      for k in 0 ..< key.len:
        if body[i + k] != byte(key[k]):
          matched = false
          break
      if matched:
        i += key.len
        break search
      inc i
    return err("no data field")

  template skipWs() =
    while i < body.len and body[i] in [byte ' ', byte '\n', byte '\r', byte '\t']:
      inc i
  template expect(c: char) =
    skipWs()
    if i >= body.len or body[i] != byte(c):
      return err("unexpected token")
    inc i

  expect(':')
  expect('[')
  var res = newSeqOfCap[BlobRef](expected)
  skipWs()
  if i < body.len and body[i] == byte(']'):
    inc i
  else:
    while true:
      expect('"')
      let start = i
      while i < body.len and body[i] != byte('"'):
        inc i
      if i >= body.len:
        return err("unterminated string")
      let blob = new Blob
      ? decodeHexInto(blob[], body.toOpenArray(start, i - 1))
      res.add blob
      inc i
      skipWs()
      if i < body.len and body[i] == byte(','):
        inc i
        continue
      expect(']')
      break
  if res.len != expected:
    return err("unexpected blob count")
  ok res

proc fetchViaBlobs(
    c: BeaconClient, slot: uint64, wanted: seq[WantedBlob]
): Future[Result[seq[BlobRef], HttpFailure]] {.async: (raises: [CancelledError]).} =
  var path = "/eth/v1/beacon/blobs/" & $slot
  for i, w in wanted:
    path.add(if i == 0: "?" else: "&")
    path.add "versioned_hashes=" & $w.versionedHash
  let body = ? await c.http.getRaw(path)

  let fast = scanBlobsResponse(body, wanted.len)
  if fast.isOk:
    return ok fast.get

  # Slow path: anything the scanner didn't expect (escapes, odd layout).
  let node = ? parseJsonBody(body)
  let data = node.jdata
  if data.isNil or data.kind != JArray or data.len != wanted.len:
    return err(HttpFailure(kind: HttpFailureKind.Decode,
      msg: "blobs: expected " & $wanted.len & " blobs"))
  var res = newSeqOfCap[BlobRef](wanted.len)
  for item in data:
    if item.kind != JString:
      return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "blobs: not a string"))
    let blob = blobFromHex(item.str).valueOr:
      return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "blobs: " & $error))
    res.add blob
  ok res

proc fetchViaSidecars(
    c: BeaconClient, slot: uint64, wanted: seq[WantedBlob]
): Future[Result[seq[BlobRef], HttpFailure]] {.async: (raises: [CancelledError]).} =
  var path = "/eth/v1/beacon/blob_sidecars/" & $slot
  for i, w in wanted:
    path.add(if i == 0: "?" else: "&")
    path.add "indices=" & $w.index
  let node = ? await c.http.getJson(path)
  let data = node.jdata
  if data.isNil or data.kind != JArray:
    return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "sidecars: no data"))

  template decodeErr(m: string): untyped =
    err(HttpFailure(kind: HttpFailureKind.Decode, msg: "sidecars: " & m))

  var res = newSeq[BlobRef](wanted.len)
  for sc in data:
    let index = parseQuantity(sc.jstr("index")).valueOr:
      return decodeErr("bad index")
    var pos = -1
    for i, w in wanted:
      if uint64(w.index) == index: pos = i
    if pos < 0:
      continue  # server ignored the indices filter; skip what we didn't ask for
    let commitment = KzgCommitment.fromHex(sc.jstr("kzg_commitment")).valueOr:
      return decodeErr("bad kzg_commitment")
    if kzgToVersionedHash(commitment) != wanted[pos].versionedHash:
      return decodeErr("commitment does not match versioned hash at index " & $index)
    res[pos] = blobFromHex(sc.jstr("blob")).valueOr:
      return decodeErr("bad blob: " & $error)
  for i, b in res:
    if b.isNil:
      return decodeErr("missing blob index " & $wanted[i].index)
  ok res

proc fetchBlobs*(
    c: BeaconClient, slot: uint64, wantedIn: seq[WantedBlob]
): Future[Result[seq[BlobRef], string]] {.async: (raises: [CancelledError]).} =
  ## Returns blobs aligned with `wantedIn` sorted by blob index.
  var wanted = wantedIn
  wanted.sort(proc(a, b: WantedBlob): int = cmp(a.index, b.index))

  case c.resolved
  of BeaconApiMode.Blobs:
    return (await c.fetchViaBlobs(slot, wanted)).mapErr(
      proc(e: HttpFailure): string = $e)
  of BeaconApiMode.Sidecars:
    return (await c.fetchViaSidecars(slot, wanted)).mapErr(
      proc(e: HttpFailure): string = $e)
  of BeaconApiMode.Auto:
    let viaBlobs = await c.fetchViaBlobs(slot, wanted)
    # Several workers may be probing concurrently; only the first one to
    # land flips the mode and logs it.
    if viaBlobs.isOk:
      if c.resolved == BeaconApiMode.Auto:
        c.resolved = BeaconApiMode.Blobs
        info "Beacon node serves /eth/v1/beacon/blobs, using it"
      return ok viaBlobs.get
    if not viaBlobs.error.endpointMissing:
      return err($viaBlobs.error)
    let viaSidecars = await c.fetchViaSidecars(slot, wanted)
    if viaSidecars.isOk:
      if c.resolved == BeaconApiMode.Auto:
        c.resolved = BeaconApiMode.Sidecars
        info "Beacon node lacks /blobs, using /blob_sidecars",
          blobsError = $viaBlobs.error
      return ok viaSidecars.get
    err("blobs: " & $viaBlobs.error & "; sidecars: " & $viaSidecars.error)
