# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Pooled JSON-over-HTTP client used for both the EL JSON-RPC endpoint and
## the beacon REST API. One `HttpSessionRef` per upstream keeps TCP
## connections alive across the parallel workers.

import
  std/[json, strutils, uri],
  chronos, chronos/apps/http/httpclient, results, stew/byteutils

export json, results, chronos

type
  HttpFailureKind* {.pure.} = enum
    Transport, Timeout, Status, Decode, Rpc

  HttpFailure* = object
    kind*: HttpFailureKind
    status*: int
    msg*: string

  JsonHttpClient* = ref object
    session: HttpSessionRef
    baseUrl: string
    timeout: Duration
    nextId: uint64

func `$`*(f: HttpFailure): string =
  case f.kind
  of HttpFailureKind.Status: "HTTP " & $f.status & ": " & f.msg
  else: $f.kind & ": " & f.msg

func failure(kind: HttpFailureKind, msg: string, status = 0): HttpFailure =
  HttpFailure(kind: kind, status: status, msg: msg)

proc new*(
    T: type JsonHttpClient, baseUrl: string, timeout: Duration,
    maxConnections = -1, bufferSize = 64 * 1024): T =
  ## `bufferSize` is the per-connection read buffer. Beacon blob responses are
  ## ~262 KiB of hex per blob, so the beacon client uses a large one.
  T(
    session: HttpSessionRef.new(
      maxConnections = maxConnections, connectionBufferSize = bufferSize),
    baseUrl: baseUrl.strip(leading = false, chars = {'/'}),
    timeout: timeout)

proc closeWait*(c: JsonHttpClient) {.async: (raises: []).} =
  await c.session.closeWait()

proc request(
    c: JsonHttpClient, meth: HttpMethod, path: string, body: seq[byte]
): Future[Result[HttpResponseTuple, HttpFailure]] {.async: (raises: [CancelledError]).} =
  let headers =
    if body.len > 0: @[("Content-Type", "application/json"), ("Accept", "application/json")]
    else: @[("Accept", "application/json")]
  let address = getHttpAddress(parseUri(c.baseUrl & path)).valueOr:
    return err(failure(HttpFailureKind.Transport, "bad url: " & $error))
  let req = HttpClientRequestRef.new(
    c.session, address, meth, headers = headers, body = body)

  var res: Result[HttpResponseTuple, HttpFailure]
  try:
    res = ok(await req.fetch().wait(c.timeout))
  except AsyncTimeoutError:
    res = err(failure(HttpFailureKind.Timeout, $meth & " " & path))
  except HttpError as exc:
    res = err(failure(HttpFailureKind.Transport, exc.msg))
  except CancelledError as exc:
    await noCancel req.closeWait()
    raise exc
  await req.closeWait()
  res

proc parseBody(data: openArray[byte]): Result[JsonNode, HttpFailure] =
  try:
    ok parseJson(string.fromBytes(data))
  except CatchableError as exc:
    err(failure(HttpFailureKind.Decode, exc.msg))

proc getRaw*(
    c: JsonHttpClient, path: string
): Future[Result[seq[byte], HttpFailure]] {.async: (raises: [CancelledError]).} =
  ## Body bytes of a 200 response, for callers with their own fast parser.
  let resp = ? await c.request(MethodGet, path, @[])
  if resp.status != 200:
    let snippet = string.fromBytes(resp.data[0 ..< min(resp.data.len, 256)])
    return err(failure(HttpFailureKind.Status, snippet, resp.status))
  ok resp.data

proc parseJsonBody*(data: openArray[byte]): Result[JsonNode, HttpFailure] =
  parseBody(data)

proc getJson*(
    c: JsonHttpClient, path: string
): Future[Result[JsonNode, HttpFailure]] {.async: (raises: [CancelledError]).} =
  let data = ? await c.getRaw(path)
  parseBody(data)

proc rpcCall*(
    c: JsonHttpClient, meth: string, params: JsonNode
): Future[Result[JsonNode, HttpFailure]] {.async: (raises: [CancelledError]).} =
  inc c.nextId
  let body = $(%*{"jsonrpc": "2.0", "id": c.nextId, "method": meth, "params": params})
  let resp = ? await c.request(MethodPost, "", body.toBytes())
  if resp.status != 200:
    return err(failure(HttpFailureKind.Status, meth, resp.status))
  let node = ? parseBody(resp.data)
  if node.kind != JObject:
    return err(failure(HttpFailureKind.Decode, "rpc response is not an object"))
  let error = node{"error"}
  if not error.isNil and error.kind != JNull:
    return err(failure(HttpFailureKind.Rpc, meth & ": " & $error))
  let res = node{"result"}
  if res.isNil:
    return err(failure(HttpFailureKind.Decode, meth & ": missing result"))
  ok res
