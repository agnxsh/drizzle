# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## drizzle entry point: wire config -> adapters -> clients -> relayer -> API.

import
  std/[strutils, options],
  chronos, chronicles, confutils,
  ./conf,
  ./l2/presets,
  ./rpc/[el_client, beacon_client],
  ./feed/[hub, server],
  ./pipeline/relayer

type DrizzleStatus {.pure.} = enum
  Running, Stopping

var status = DrizzleStatus.Running

proc controlCHandler() {.noconv.} =
  when defined(windows):
    # workaround for https://github.com/nim-lang/Nim/issues/4057
    try: setupForeignThreadGc()
    except Exception as exc: raiseAssert exc.msg
  notice "Shutting down after having received SIGINT"
  status = DrizzleStatus.Stopping

proc setupLogLevel(level: string) =
  try:
    setLogLevel(parseEnum[LogLevel](level.toUpperAscii))
  except ValueError:
    fatal "Invalid log level", level
    quit QuitFailure

proc waitForBeacon(beacon: BeaconClient) {.async: (raises: [CancelledError]).} =
  ## The beacon node might still be starting; keep trying instead of exiting.
  var delay = 1.seconds
  while status == DrizzleStatus.Running:
    let res = await beacon.init()
    if res.isOk:
      info "Connected to beacon node", genesisTime = beacon.genesisTime,
        secondsPerSlot = beacon.secondsPerSlot
      return
    warn "Beacon node not ready, retrying", error = res.error, retryIn = delay
    await sleepAsync(delay)
    delay = min(delay * 2, 30.seconds)

proc run(config: DrizzleConf) {.raises: [CatchableError].} =
  setupLogLevel(config.logLevel)

  if config.feeds.len == 0:
    fatal "At least one --feed is required"
    quit QuitFailure

  var adapters: seq[L2Adapter]
  for spec in config.feeds:
    let a = parseFeedSpec(spec).valueOr:
      fatal "Invalid --feed", spec, error
      quit QuitFailure
    info "Feed configured", name = a.name, kind = a.kind, inbox = $a.inbox,
      batchers = a.batchers.len
    adapters.add a

  let index = AdapterIndex.init(adapters).valueOr:
    fatal "Invalid feed set", error
    quit QuitFailure

  let mode = parseBeaconApiMode(config.beaconApi).valueOr:
    fatal "Invalid --beacon-api", error
    quit QuitFailure

  if config.workers < 1 or config.ringSize < 1 or config.subscriberQueue < 1:
    fatal "--workers, --ring-size and --subscriber-queue must be positive"
    quit QuitFailure

  let
    timeout = config.requestTimeoutMs.milliseconds
    el = ElClient.new(config.elUrl, timeout, config.maxUpstreamConnections)
    beacon = BeaconClient.new(
      config.beaconUrl, timeout, mode, config.maxUpstreamConnections)
    hub = FeedHub.new(adapters, config.ringSize, config.subscriberQueue)
    rcfg = RelayerConfig(
      workers: config.workers,
      windowPerWorker: 4,
      confirmations: config.confirmations,
      pollInterval: config.pollIntervalMs.milliseconds,
      startBlock:
        if config.startBlock.isSome: Opt.some(config.startBlock.get)
        else: Opt.none(uint64),
      retainBlobs: config.retainBlobs)
    relayer = Relayer.new(rcfg, el, beacon, index, hub)

  let apiAddr = initTAddress(config.apiAddress, Port(config.apiPort))
  let api = ApiServer.new(
      apiAddr, hub, relayer, config.heartbeatSecs.seconds,
      config.apiMaxConnections).valueOr:
    fatal "Failed to start feed API", address = $apiAddr, error
    quit QuitFailure

  setControlCHook(controlCHandler)

  waitFor waitForBeacon(beacon)
  if status != DrizzleStatus.Running:
    return

  api.start()
  info "Feed API listening", address = $api.localAddress
  relayer.start()

  while status == DrizzleStatus.Running:
    poll()

  waitFor relayer.stop()
  waitFor api.stop()
  waitFor el.http.closeWait()
  waitFor beacon.http.closeWait()
  notice "drizzle stopped"

when isMainModule:
  let config =
    try:
      DrizzleConf.load(version = DrizzleVersion, copyrightBanner =
        "drizzle " & DrizzleVersion & " - pluggable L2 blob feed relayer\n" &
        "Copyright (c) 2026 Agnish Ghosh. Licensed under MIT or Apache-2.0.")
    except CatchableError as exc:
      stderr.writeLine "Failed to load configuration: " & exc.msg
      quit QuitFailure
  try:
    run(config)
  except CatchableError as exc:
    fatal "Unhandled error", error = exc.msg
    quit QuitFailure
