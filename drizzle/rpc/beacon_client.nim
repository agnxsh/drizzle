# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Beacon REST API client: genesis/spec for timestamp -> slot, and blob
## retrieval via `/eth/v1/beacon/blobs` (post-Fulu) or
## `/eth/v1/beacon/blob_sidecars`, across several endpoints.
##
## Endpoints come in two roles:
## * Live (`--beacon-url`, repeatable, in priority order). Healthy endpoints
##   that serve full blobs are tried first. One that times out, returns 5xx or
##   rate-limits (429) is put on a cooldown (Retry-After if given, otherwise
##   exponential backoff) and the next one is tried.
## * Archive (`--beacon-archive-url`). Only asked for slots the live nodes
##   no longer have (beacon nodes prune blobs after
##   MIN_EPOCHS_FOR_*_SIDECARS_REQUESTS, about 18 days). Requests go by block
##   root, because blob archivers index by root; the root comes from a live
##   node's headers, which are never pruned.
##
## Failures are classified so the relayer can react properly: wait out a
## rate limit, retry shortly at the chain head, or explain that blobs were
## pruned instead of retrying blindly. See NOTES.md section 6.
##
## Spec: https://ethereum.github.io/beacon-APIs/
##   (getBlobs: /eth/v1/beacon/blobs, getBlobSidecars: /eth/v1/beacon/blob_sidecars,
##    getBlockHeader: /eth/v1/beacon/headers, getNetworkIdentity: /eth/v1/node/identity)
## PeerDAS custody: https://github.com/ethereum/consensus-specs/blob/master/specs/fulu/das-core.md

import
  std/[algorithm, strutils],
  chronicles,
  ./json_http, ../spec/eip4844

from std/times import getTime, toUnix  # std/times' Duration clashes with chronos'

export json_http

logScope:
  topics = "beacon"

const
  DefaultRetentionEpochs = 4096'u64  ## MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS
  DefaultSlotsPerEpoch = 32'u64
  DefaultCustodyGroups = 128'u64     ## NUMBER_OF_CUSTODY_GROUPS
  HeadGraceSlots = 2'u64
    ## A beacon node can lag the EL by a moment at the head; a miss this close
    ## to the current slot is "not yet", not an error.
  MinCooldown = 1.seconds
  MaxCooldown = 60.seconds
  ProbeDepth = 8                     ## slots to search back for a blob-carrying block

type
  BeaconApiMode* {.pure.} = enum
    Auto, Blobs, Sidecars

  EndpointRole* {.pure.} = enum
    Live, Archive

  EndpointStatus* {.pure.} = enum
    ## What the startup probe learned about a live endpoint.
    Unknown       ## not probed, or no recent blobs to test with
    Full          ## served a complete set of blobs
    Partial       ## block exists but blobs weren't served (PeerDAS custody)
    WrongNetwork  ## different genesis than the first endpoint: never used

  WantedBlob* = object
    index*: int                  ## blob index within the beacon block
    versionedHash*: VersionedHash

  BeaconEndpoint* = ref object
    http*: JsonHttpClient
    name*: string                ## scheme://host:port, safe to log
    role*: EndpointRole
    resolved*: BeaconApiMode     ## Auto until a request settles it
    status*: EndpointStatus
    custodyGroups*: Opt[uint64]
    failures*: int               ## consecutive retryable failures
    cooldownUntil*: Moment
    prunedBelow: uint64
      ## Live only: this endpoint answered "not found" for an expired slot
      ## `prunedBelow - 1`, so anything older is gone as well; don't ask.

  BlobFetchErrorKind* {.pure.} = enum
    Unavailable  ## endpoints failing or cooling down: retry later
    NotYet       ## very recent slot not served yet: retry soon, not an error
    Pruned       ## past every endpoint's retention: needs an archive
    Invalid      ## an endpoint returned data that doesn't match the block

  BlobFetchError* = object
    kind*: BlobFetchErrorKind
    msg*: string
    retryAfter*: Opt[Duration]   ## when the first cooling endpoint frees up

  BeaconClient* = ref object
    endpoints*: seq[BeaconEndpoint]
    mode*: BeaconApiMode
    genesisTime*: uint64
    secondsPerSlot*: uint64
    slotsPerEpoch*: uint64
    retentionEpochs*: uint64
    custodyGroupsTotal*: uint64
    clock*: proc(): uint64 {.gcsafe, raises: [].}
      ## Wall clock, unix seconds. Tests replace it.

func `$`*(e: BlobFetchError): string = $e.kind & ": " & e.msg

func parseBeaconApiMode*(s: string): Result[BeaconApiMode, string] =
  case s.toLowerAscii
  of "auto": ok BeaconApiMode.Auto
  of "blobs": ok BeaconApiMode.Blobs
  of "sidecars": ok BeaconApiMode.Sidecars
  else: err("beacon api mode must be auto, blobs or sidecars")

proc systemClock(): uint64 {.gcsafe, raises: [].} =
  uint64(max(0'i64, getTime().toUnix()))

proc new*(
    T: type BeaconClient, urls: openArray[string], timeout: Duration,
    mode = BeaconApiMode.Auto, maxConnections = -1,
    archiveUrls: openArray[string] = []): T =
  ## `urls` in priority order; `archiveUrls` only serve pruned slots.
  doAssert urls.len > 0, "at least one beacon url"
  result = T(mode: mode, slotsPerEpoch: DefaultSlotsPerEpoch,
             retentionEpochs: DefaultRetentionEpochs,
             custodyGroupsTotal: DefaultCustodyGroups, clock: systemClock)
  template add(url: string, r: EndpointRole) =
    result.endpoints.add BeaconEndpoint(
      http: JsonHttpClient.new(url, timeout, maxConnections, bufferSize = 1024 * 1024),
      name: displayName(url), role: r, resolved: mode)
  for u in urls: add(u, EndpointRole.Live)
  for u in archiveUrls: add(u, EndpointRole.Archive)

proc new*(
    T: type BeaconClient, url: string, timeout: Duration,
    mode = BeaconApiMode.Auto, maxConnections = -1): T =
  BeaconClient.new([url], timeout, mode, maxConnections)

proc closeWait*(c: BeaconClient) {.async: (raises: []).} =
  for ep in c.endpoints:
    await ep.http.closeWait()

func hasArchive*(c: BeaconClient): bool =
  for ep in c.endpoints:
    if ep.role == EndpointRole.Archive: return true

func jdata(n: JsonNode): JsonNode =
  if n.kind == JObject: n{"data"} else: nil

func jstr(n: JsonNode, key: string): string =
  if n.isNil or n.kind != JObject: return ""
  let v = n{key}
  if v.isNil or v.kind != JString: "" else: v.str

# Time ------------------------------------------------------------------------

func slotAt*(c: BeaconClient, timestamp: uint64): Result[uint64, string] =
  if timestamp < c.genesisTime:
    return err("timestamp before genesis")
  let delta = timestamp - c.genesisTime
  if delta mod c.secondsPerSlot != 0:
    return err("timestamp not aligned to slot boundary")
  ok delta div c.secondsPerSlot

proc currentSlot*(c: BeaconClient): uint64 =
  let now = c.clock()
  if now <= c.genesisTime or c.secondsPerSlot == 0: 0'u64
  else: (now - c.genesisTime) div c.secondsPerSlot

func retentionSlots*(c: BeaconClient): uint64 =
  c.retentionEpochs * c.slotsPerEpoch

proc isExpired*(c: BeaconClient, slot: uint64): bool =
  ## Older than the spec's minimum blob retention. Nodes may keep blobs
  ## longer (archive settings) but aren't required to.
  c.currentSlot() > slot + c.retentionSlots

proc isRecent(c: BeaconClient, slot: uint64): bool =
  slot + HeadGraceSlots >= c.currentSlot()

func retentionText(c: BeaconClient): string =
  let days = float(c.retentionSlots * c.secondsPerSlot) / 86400.0
  $c.retentionEpochs & " epochs, ~" & formatFloat(days, ffDecimal, 1) & " days"

# Setup -----------------------------------------------------------------------

proc specValue(spec: JsonNode, key: string, default: uint64): uint64 =
  parseQuantity(spec.jdata.jstr(key)).get(default)

proc init*(
    c: BeaconClient
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  ## Genesis and spec from the first live endpoint that answers.
  var errors: seq[string]
  for ep in c.endpoints:
    if ep.role != EndpointRole.Live: continue
    let genesis = (await ep.http.getJson("/eth/v1/beacon/genesis")).valueOr:
      errors.add ep.name & ": genesis: " & $error
      continue
    let genesisTime = parseQuantity(genesis.jdata.jstr("genesis_time")).valueOr:
      errors.add ep.name & ": genesis: bad genesis_time"
      continue
    let spec = (await ep.http.getJson("/eth/v1/config/spec")).valueOr:
      errors.add ep.name & ": spec: " & $error
      continue
    let sps = spec.specValue("SECONDS_PER_SLOT", 0)
    if sps == 0:
      errors.add ep.name & ": spec: bad SECONDS_PER_SLOT"
      continue
    c.genesisTime = genesisTime
    c.secondsPerSlot = sps
    c.slotsPerEpoch = spec.specValue("SLOTS_PER_EPOCH", DefaultSlotsPerEpoch)
    # Fulu keeps columns for MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS;
    # before that, blobs for MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS.
    c.retentionEpochs = spec.specValue("MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS",
      spec.specValue("MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS", DefaultRetentionEpochs))
    c.custodyGroupsTotal = spec.specValue("NUMBER_OF_CUSTODY_GROUPS", DefaultCustodyGroups)
    return ok()
  err(errors.join("; "))

# Fetching from one endpoint ---------------------------------------------------

func endpointMissing(f: HttpFailure): bool =
  f.kind == HttpFailureKind.Status and f.status in [400, 404, 405, 501]

const MissingTag = "missing: "

func missing(msg: string): HttpFailure =
  ## The server answered but doesn't have (all) the data.
  HttpFailure(kind: HttpFailureKind.Decode, msg: MissingTag & msg)

func isMissing(f: HttpFailure): bool =
  (f.kind == HttpFailureKind.Status and f.status in [404, 410]) or
    (f.kind == HttpFailureKind.Decode and f.msg.startsWith(MissingTag))

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
    http: JsonHttpClient, blockId: string, wanted: seq[WantedBlob]
): Future[Result[seq[BlobRef], HttpFailure]] {.async: (raises: [CancelledError]).} =
  var path = "/eth/v1/beacon/blobs/" & blockId
  for i, w in wanted:
    path.add(if i == 0: "?" else: "&")
    path.add "versioned_hashes=" & $w.versionedHash
  let body = ? await http.getRaw(path)

  let fast = scanBlobsResponse(body, wanted.len)
  if fast.isOk:
    return ok fast.get

  # Slow path: anything the scanner didn't expect (escapes, odd layout).
  let node = ? parseJsonBody(body)
  let data = node.jdata
  if data.isNil or data.kind != JArray:
    return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "blobs: no data"))
  if data.len != wanted.len:
    return err(missing("blobs: got " & $data.len & " of " & $wanted.len))
  var res = newSeqOfCap[BlobRef](wanted.len)
  for item in data:
    if item.kind != JString:
      return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "blobs: not a string"))
    let blob = blobFromHex(item.str).valueOr:
      return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "blobs: " & $error))
    res.add blob
  ok res

proc fetchViaSidecars(
    http: JsonHttpClient, blockId: string, wanted: seq[WantedBlob]
): Future[Result[seq[BlobRef], HttpFailure]] {.async: (raises: [CancelledError]).} =
  var path = "/eth/v1/beacon/blob_sidecars/" & blockId
  for i, w in wanted:
    path.add(if i == 0: "?" else: "&")
    path.add "indices=" & $w.index
  let node = ? await http.getJson(path)
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
      return err(missing("sidecars: blob index " & $wanted[i].index))
  ok res

proc fetchFrom(
    ep: BeaconEndpoint, blockId: string, wanted: seq[WantedBlob]
): Future[Result[seq[BlobRef], HttpFailure]] {.async: (raises: [CancelledError]).} =
  case ep.resolved
  of BeaconApiMode.Blobs:
    await fetchViaBlobs(ep.http, blockId, wanted)
  of BeaconApiMode.Sidecars:
    await fetchViaSidecars(ep.http, blockId, wanted)
  of BeaconApiMode.Auto:
    let viaBlobs = await fetchViaBlobs(ep.http, blockId, wanted)
    # Several workers may be probing concurrently; only the first one to
    # land flips the mode and logs it.
    if viaBlobs.isOk:
      if ep.resolved == BeaconApiMode.Auto:
        ep.resolved = BeaconApiMode.Blobs
        info "Beacon endpoint serves /eth/v1/beacon/blobs, using it", endpoint = ep.name
      return viaBlobs
    if not viaBlobs.error.endpointMissing:
      return viaBlobs
    let viaSidecars = await fetchViaSidecars(ep.http, blockId, wanted)
    if viaSidecars.isOk:
      if ep.resolved == BeaconApiMode.Auto:
        ep.resolved = BeaconApiMode.Sidecars
        info "Beacon endpoint lacks /blobs, using /blob_sidecars",
          endpoint = ep.name, blobsError = $viaBlobs.error
      return viaSidecars
    if viaBlobs.error.isMissing and viaSidecars.error.isMissing:
      return err(missing("blobs: " & $viaBlobs.error & "; sidecars: " & $viaSidecars.error))
    # Report the more informative of the two.
    if viaSidecars.error.retryable: viaSidecars else: viaBlobs

# Endpoint health --------------------------------------------------------------

proc penalize(ep: BeaconEndpoint, f: HttpFailure) =
  inc ep.failures
  let backoff = min(MinCooldown * (1 shl min(ep.failures - 1, 6)), MaxCooldown)
  let wait = f.retryAfter.get(backoff)
  ep.cooldownUntil = Moment.now() + wait
  if ep.failures == 1 or f.retryAfter.isSome:
    warn "Beacon endpoint failing, backing off", endpoint = ep.name,
      error = $f, retryIn = wait, consecutiveFailures = ep.failures
  else:
    debug "Beacon endpoint still failing", endpoint = ep.name,
      error = $f, retryIn = wait, consecutiveFailures = ep.failures

proc recovered(ep: BeaconEndpoint) =
  if ep.failures > 0:
    info "Beacon endpoint recovered", endpoint = ep.name,
      afterFailures = ep.failures
    ep.failures = 0

func usable(ep: BeaconEndpoint): bool =
  ep.status != EndpointStatus.WrongNetwork

proc liveOrder(c: BeaconClient): seq[BeaconEndpoint] =
  ## Configured order, but endpoints known to serve full blobs first and
  ## ones known not to last (they're still tried: the probe can be wrong).
  for st in [EndpointStatus.Full, EndpointStatus.Unknown, EndpointStatus.Partial]:
    for ep in c.endpoints:
      if ep.role == EndpointRole.Live and ep.status == st:
        result.add ep

# Fetching across endpoints -----------------------------------------------------

type Tally = ref object
  retryable, missing, invalid: bool
  lastMissing: bool    ## the most recent attempt was a "not found"
  errors: seq[string]

proc attempt(
    ep: BeaconEndpoint, blockId: string, wanted: seq[WantedBlob], t: Tally
): Future[Opt[seq[BlobRef]]] {.async: (raises: [CancelledError]).} =
  t.lastMissing = false
  let res = await ep.fetchFrom(blockId, wanted)
  if res.isOk:
    ep.recovered()
    return Opt.some(res.get)
  let f = res.error
  t.errors.add ep.name & ": " & $f
  if f.retryable:
    ep.penalize(f)
    t.retryable = true
  elif f.isMissing:
    t.missing = true
    t.lastMissing = true
  else:
    t.invalid = true
  Opt.none(seq[BlobRef])

proc blockRoot(
    c: BeaconClient, slot: uint64
): Future[Result[string, HttpFailure]] {.async: (raises: [CancelledError]).} =
  ## Block root for `slot` from any live endpoint. Headers aren't pruned.
  var last = HttpFailure(kind: HttpFailureKind.Transport, msg: "no live endpoint available")
  let now = Moment.now()
  for ep in c.liveOrder():
    if ep.cooldownUntil > now: continue
    let res = await ep.http.getJson("/eth/v1/beacon/headers/" & $slot)
    if res.isErr:
      last = res.error
      if res.error.retryable:
        ep.penalize(res.error)
        continue
      return err(res.error)
    let root = res.get.jdata.jstr("root")
    if root.len != 66 or not root.startsWith("0x"):
      return err(HttpFailure(kind: HttpFailureKind.Decode, msg: "headers: bad root"))
    return ok root
  err(last)

proc earliestCooldown(c: BeaconClient): Opt[Duration] =
  let now = Moment.now()
  var best = Opt.none(Duration)
  for ep in c.endpoints:
    if ep.usable and ep.cooldownUntil > now:
      let left = ep.cooldownUntil - now
      if best.isNone or left < best.get: best = Opt.some(left)
  best

proc fetchBlobs*(
    c: BeaconClient, slot: uint64, wantedIn: seq[WantedBlob]
): Future[Result[seq[BlobRef], BlobFetchError]] {.async: (raises: [CancelledError]).} =
  ## Returns blobs aligned with `wantedIn` sorted by blob index.
  var wanted = wantedIn
  wanted.sort(proc(a, b: WantedBlob): int = cmp(a.index, b.index))
  let
    expired = c.isExpired(slot)
    recent = c.isRecent(slot)
    now = Moment.now()
    t = Tally()

  for ep in c.liveOrder():
    if expired and slot < ep.prunedBelow:
      t.missing = true
      continue
    if ep.cooldownUntil > now:
      t.retryable = true
      continue
    let got = await ep.attempt($slot, wanted, t)
    if got.isSome:
      return ok got.get
    if expired and t.lastMissing:
      ep.prunedBelow = max(ep.prunedBelow, slot + 1)

  # Archives: for slots past retention, or ones every live node lacks.
  if c.hasArchive and (expired or (t.missing and not recent)):
    let root = await c.blockRoot(slot)
    if root.isErr:
      t.errors.add "block root: " & $root.error
      if root.error.retryable: t.retryable = true
    else:
      for ep in c.endpoints:
        if ep.role != EndpointRole.Archive: continue
        if ep.cooldownUntil > Moment.now():
          t.retryable = true
          continue
        let got = await ep.attempt(root.get, wanted, t)
        if got.isSome:
          return ok got.get

  let detail = t.errors.join("; ")
  if t.missing and expired and not t.retryable:
    return err(BlobFetchError(kind: BlobFetchErrorKind.Pruned, msg:
      "blobs for slot " & $slot & " are past beacon retention (" & c.retentionText & ")" &
      (if c.hasArchive: " and no archive has them" else: "; add --beacon-archive-url") &
      (if detail.len > 0: " [" & detail & "]" else: "")))
  if t.missing and recent and not t.retryable:
    return err(BlobFetchError(kind: BlobFetchErrorKind.NotYet,
      msg: "slot " & $slot & " blobs not served yet"))
  if t.retryable:
    return err(BlobFetchError(kind: BlobFetchErrorKind.Unavailable,
      msg: (if detail.len > 0: detail else: "all beacon endpoints cooling down"),
      retryAfter: c.earliestCooldown()))
  if t.invalid:
    return err(BlobFetchError(kind: BlobFetchErrorKind.Invalid, msg: detail))
  if t.missing:
    return err(BlobFetchError(kind: BlobFetchErrorKind.Unavailable, msg:
      detail & " (blobs inside retention but not served: since PeerDAS a beacon " &
      "node must custody at least half the columns, e.g. run as a supernode)"))
  err(BlobFetchError(kind: BlobFetchErrorKind.Unavailable, msg: "no usable beacon endpoint"))

# Startup probe ----------------------------------------------------------------

proc probeEndpoint(
    c: BeaconClient, ep: BeaconEndpoint
) {.async: (raises: [CancelledError]).} =
  # Same network?
  let genesis = await ep.http.getJson("/eth/v1/beacon/genesis")
  if genesis.isOk:
    let g = parseQuantity(genesis.get.jdata.jstr("genesis_time"))
    if g.isOk and g.get != c.genesisTime:
      ep.status = EndpointStatus.WrongNetwork
      error "Beacon endpoint is on a different network, ignoring it",
        endpoint = ep.name, genesisTime = g.get, expected = c.genesisTime
      return
  else:
    warn "Beacon endpoint unreachable during probe", endpoint = ep.name,
      error = $genesis.error
    return

  # How many PeerDAS custody groups does it keep? (Fulu metadata field.)
  let ident = await ep.http.getJson("/eth/v1/node/identity")
  if ident.isOk:
    let meta = ident.get.jdata
    if not meta.isNil and meta.kind == JObject:
      let cgc = parseQuantity(meta{"metadata"}.jstr("custody_group_count"))
      if cgc.isOk: ep.custodyGroups = Opt.some(cgc.get)

  # Functional test: ask for every blob of a recent block that has some.
  let head = c.currentSlot()
  var verdict = EndpointStatus.Unknown
  for back in 1 .. ProbeDepth:
    if head < uint64(back): break
    let slot = head - uint64(back)
    let blobs = await ep.http.getRaw("/eth/v1/beacon/blobs/" & $slot)
    if blobs.isOk:
      let n = block:
        let node = parseJsonBody(blobs.get)
        if node.isErr or node.get.jdata.isNil or node.get.jdata.kind != JArray: -1
        else: node.get.jdata.len
      if n > 0:
        verdict = EndpointStatus.Full
        if ep.resolved == BeaconApiMode.Auto: ep.resolved = BeaconApiMode.Blobs
        break
      continue  # empty block, look further back
    if blobs.error.retryable:
      continue
    # Not served. Is there a block at all, and does the old endpoint work?
    let header = await ep.http.getJson("/eth/v1/beacon/headers/" & $slot)
    if header.isErr:
      continue  # missed slot
    let sidecars = await ep.http.getJson("/eth/v1/beacon/blob_sidecars/" & $slot)
    if sidecars.isOk:
      let data = sidecars.get.jdata
      if not data.isNil and data.kind == JArray:
        if data.len == 0: continue
        verdict = EndpointStatus.Full
        if ep.resolved == BeaconApiMode.Auto: ep.resolved = BeaconApiMode.Sidecars
        break
    verdict = EndpointStatus.Partial
    break

  # Custody count settles it when the functional test couldn't.
  if verdict == EndpointStatus.Unknown and ep.custodyGroups.isSome:
    verdict =
      if ep.custodyGroups.get * 2 >= c.custodyGroupsTotal: EndpointStatus.Full
      else: EndpointStatus.Partial
  ep.status = verdict

  let custody =
    if ep.custodyGroups.isSome: $ep.custodyGroups.get & "/" & $c.custodyGroupsTotal
    else: "unknown"
  case verdict
  of EndpointStatus.Full:
    info "Beacon endpoint serves full blobs", endpoint = ep.name,
      api = $ep.resolved, custodyGroups = custody
  of EndpointStatus.Partial:
    warn "Beacon endpoint can't serve full blobs. Since PeerDAS (Fulu) only " &
      "nodes custodying at least half the column groups can (e.g. supernodes); " &
      "it will be used as a last resort", endpoint = ep.name, custodyGroups = custody
  of EndpointStatus.Unknown:
    info "Couldn't verify blob service (no recent blobs to test with)",
      endpoint = ep.name, custodyGroups = custody
  of EndpointStatus.WrongNetwork:
    discard

proc probe*(c: BeaconClient) {.async: (raises: [CancelledError]).} =
  ## One-off startup check of every live endpoint: same network, PeerDAS
  ## custody, and whether it actually returns full blobs. Costs one block's
  ## worth of blobs per endpoint. Results only reorder endpoints and drop
  ## wrong-network ones; nothing is fatal.
  for ep in c.endpoints:
    if ep.role == EndpointRole.Live:
      await c.probeEndpoint(ep)
    else:
      info "Beacon archive configured", endpoint = ep.name
  var good = false
  for ep in c.endpoints:
    if ep.role == EndpointRole.Live and
        ep.status in [EndpointStatus.Full, EndpointStatus.Unknown]:
      good = true
  if not good:
    warn "No live beacon endpoint is known to serve full blobs; blob fetches " &
      "will likely fail. Point --beacon-url at a supernode or a provider that serves blobs"
