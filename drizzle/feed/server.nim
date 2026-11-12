# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Consumer-facing HTTP API.
##
##   GET /                                                        (dashboard)
##   GET /health
##   GET /v1/status
##   GET /v1/feeds
##   GET /v1/feeds/{name}/events?from=<seq>&limit=<n>&blobs=0|1      (JSON, poll)
##   GET /v1/feeds/{name}/stream?from=<seq>&format=bin|json|summary&blobs=0|1
##                                                                (chunked stream)
##
## Streams send a heartbeat frame when idle so dead peers are noticed, and
## end with a `Lagged` frame if the consumer falls `--subscriber-queue`
## events behind. Reconnect with `from = lastSeq + 1`.

import
  std/[json, strutils],
  chronos, chronos/apps/http/httpserver, chronicles,
  ./[hub, wire],
  ../pipeline/relayer,
  ../conf

const
  DashboardHtml = staticRead("dashboard.html")
    ## Single self-contained page (no external assets), baked into the binary.

export httpserver

logScope:
  topics = "api"

type
  ApiServer* = ref object
    server: HttpServerRef
    hub: FeedHub
    relayer: Relayer
    heartbeat: Duration
    maxPollLimit: int
    startedAt: Moment

func parseUintParam(req: HttpRequestRef, key: string): Result[Opt[uint64], string] =
  if key notin req.query:
    return ok(Opt.none(uint64))
  let s = req.query.getString(key)
  try:
    ok(Opt.some(parseBiggestUInt(s).uint64))
  except ValueError:
    err("invalid '" & key & "' parameter")

func flagParam(req: HttpRequestRef, key: string): bool =
  key in req.query and req.query.getString(key) in ["1", "true", "yes"]

proc respondJson(
    req: HttpRequestRef, code: HttpCode, node: JsonNode
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  let headers = HttpTable.init([("Content-Type", "application/json")])
  try:
    await req.respond(code, $node, headers)
  except HttpWriteError as exc:
    defaultResponse(exc)

proc respondError(
    req: HttpRequestRef, code: HttpCode, msg: string
): Future[HttpResponseRef] {.async: (raises: [CancelledError], raw: true).} =
  req.respondJson(code, %*{"error": msg})

proc beaconJson(s: ApiServer): JsonNode =
  result = newJArray()
  let now = Moment.now()
  for ep in s.relayer.beacon.endpoints:
    result.add %*{
      "endpoint": ep.name, "role": $ep.role, "api": $ep.resolved,
      "status": $ep.status,
      "custodyGroups":
        if ep.custodyGroups.isSome: %ep.custodyGroups.get else: newJNull(),
      "consecutiveFailures": ep.failures,
      "coolingDownMs":
        if ep.cooldownUntil > now: (ep.cooldownUntil - now).milliseconds else: 0,
    }

proc statusJson(s: ApiServer): JsonNode =
  let st = s.relayer.stats
  var feeds = newJObject()
  for name, f in s.hub.feeds:
    feeds[name] = %*{
      "kind": f.adapter.kind,
      "inbox": $f.adapter.inbox,
      "headSeq": f.headSeq,
      "oldestSeq": f.oldestSeq,
      "subscribers": f.subscriberCount,
      "published": f.stats.published,
      "payloadBytes": f.stats.payloadBytes,
      "laggedDrops": f.stats.laggedDrops,
    }
  %*{
    "version": DrizzleVersion,
    "uptimeSecs": (Moment.now() - s.startedAt).seconds,
    "workers": s.relayer.workers,
    "elHead": st.elHead, "safeHead": st.safeHead, "nextBlock": st.nextEmit,
    "blocksProcessed": st.blocksProcessed, "blobTxsMatched": st.blobTxsMatched,
    "blobsFetched": st.blobsFetched, "eventsPublished": st.eventsPublished,
    "decodeErrors": st.decodeErrors, "fetchErrors": st.fetchErrors,
    "reorgs": st.reorgs,
    "stalledAt": st.stalledAt, "stallReason": st.stallReason,
    "beacon": s.beaconJson(), "feeds": feeds,
  }

proc streamFeed(
    s: ApiServer, req: HttpRequestRef, feed: Feed
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  let fromSeq = parseUintParam(req, "from").valueOr:
    return await req.respondError(Http400, error)
  let format = req.query.getString("format", "bin")
  if format notin ["bin", "json", "summary"]:
    return await req.respondError(Http400, "format must be bin, json or summary")
  let variant =
    if format == "summary": WireVariant.JsonSummary
    else: wireVariant(format == "json", req.flagParam("blobs"))

  let sub = feed.subscribe(fromSeq).valueOr:
    return await req.respondError(Http410,
      "requested seq evicted from ring, oldest available is " & $feed.oldestSeq)

  let resp = req.getResponse()
  resp.status = Http200
  resp.setHeader("Content-Type",
    if format == "bin": "application/octet-stream" else: "application/x-ndjson")
  resp.setHeader("Cache-Control", "no-cache")
  resp.setHeader("X-Drizzle-Wire-Version", $WireVersion)
  resp.setHeader("X-Drizzle-Head-Seq", $feed.headSeq)

  debug "Subscriber connected", feed = feed.name, remote = $req.remote,
    fromSeq, format
  try:
    await resp.prepare()
    while true:
      let ev =
        try:
          await sub.queue.popFirst().wait(s.heartbeat)
        except AsyncTimeoutError:
          FeedEvent(kind: FeedEventKind.Heartbeat, feed: feed.name)
      # Send straight from the cached encoding. `ev` stays referenced
      # across the await, so the buffer can't be freed underneath us.
      let frame = addr ev.encode(variant)
      await resp.send(unsafeAddr frame[][0], frame[].len)
      if ev.kind == FeedEventKind.Lagged:
        info "Subscriber lagged, disconnecting", feed = feed.name,
          remote = $req.remote
        break
    await resp.finish()
  except HttpWriteError as exc:
    debug "Subscriber disconnected", feed = feed.name, reason = exc.msg
  finally:
    feed.unsubscribe(sub)
  resp

proc pollFeed(
    s: ApiServer, req: HttpRequestRef, feed: Feed
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  let fromSeq = parseUintParam(req, "from").valueOr:
    return await req.respondError(Http400, error)
  let limitParam = parseUintParam(req, "limit").valueOr:
    return await req.respondError(Http400, error)
  let
    start = fromSeq.get(feed.oldestSeq)
    limit = int(min(limitParam.get(100'u64), uint64(s.maxPollLimit)))
    withBlobs = req.flagParam("blobs")
  if start < feed.oldestSeq and start <= feed.headSeq:
    return await req.respondError(Http410,
      "requested seq evicted from ring, oldest available is " & $feed.oldestSeq)
  var events = newJArray()
  for ev in feed.eventsSince(start, limit):
    events.add ev.toJsonNode(withBlobs)
  await req.respondJson(Http200, %*{
    "headSeq": feed.headSeq, "oldestSeq": feed.oldestSeq, "events": events})

proc route(
    s: ApiServer, req: HttpRequestRef
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  if req.meth != MethodGet:
    return await req.respondError(Http405, "only GET is supported")
  let parts = req.uri.path.strip(chars = {'/'}).split('/')
  if parts == @[""] or parts == @["dashboard"]:
    let headers = HttpTable.init([
      ("Content-Type", "text/html; charset=utf-8"),
      ("Cache-Control", "no-cache")])
    try:
      return await req.respond(Http200, DashboardHtml, headers)
    except HttpWriteError as exc:
      return defaultResponse(exc)
  if parts == @["health"]:
    return await req.respondJson(Http200, %*{"status": "ok"})
  if parts == @["v1", "status"]:
    return await req.respondJson(Http200, s.statusJson())
  if parts == @["v1", "feeds"]:
    var feeds = newJArray()
    for name, f in s.hub.feeds:
      feeds.add %*{"name": name, "kind": f.adapter.kind,
                   "inbox": $f.adapter.inbox, "headSeq": f.headSeq}
    return await req.respondJson(Http200, feeds)
  if parts.len == 4 and parts[0] == "v1" and parts[1] == "feeds":
    let feed = s.hub.getFeed(parts[2]).valueOr:
      return await req.respondError(Http404, "unknown feed '" & parts[2] & "'")
    case parts[3]
    of "stream": return await s.streamFeed(req, feed)
    of "events": return await s.pollFeed(req, feed)
    else: discard
  await req.respondError(Http404, "not found")

proc new*(
    T: type ApiServer, address: TransportAddress, hub: FeedHub,
    relayer: Relayer, heartbeat: Duration, maxConnections: int
): Result[T, string] =
  let s = T(hub: hub, relayer: relayer, heartbeat: heartbeat, maxPollLimit: 1000,
            startedAt: Moment.now())

  proc process(r: RequestFence): Future[HttpResponseRef] {.
      async: (raises: [CancelledError]).} =
    let req = r.valueOr:
      return defaultResponse(error)
    await s.route(req)

  s.server = ? HttpServerRef.new(
    address, process, maxConnections = maxConnections)
  ok s

proc start*(s: ApiServer) =
  s.server.start()

func localAddress*(s: ApiServer): TransportAddress =
  s.server.instance.localAddress()

proc stop*(s: ApiServer) {.async: (raises: []).} =
  await s.server.stop()
  await s.server.closeWait()
