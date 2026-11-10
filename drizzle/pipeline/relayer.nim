# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## The relay pipeline:
##
##   headLoop ──► schedulerLoop ──work──► workerLoop x N ──results──► emitterLoop ──► FeedHub
##                (bounded window)        (EL block, filter,         (reorder buffer,
##                                         beacon blobs, decode)      reorg detection)
##
## Everything runs on one chronos event loop; concurrency is I/O overlap
## across workers. See NOTES.md sections 4 and 5.

import
  std/[deques, tables],
  chronos, chronicles,
  ../rpc/[el_client, beacon_client],
  ../l2/adapter,
  ../feed/hub,
  ./reorder

logScope:
  topics = "relayer"

const
  RecentHashes = 256
  MaxRetryDelay = 5.seconds

type
  RelayerConfig* = object
    workers*: int
    windowPerWorker*: int        ## scheduler look-ahead = workers * this
    confirmations*: uint64
    pollInterval*: Duration
    startBlock*: Opt[uint64]
    retainBlobs*: bool

  RelayerStats* = object
    elHead*, safeHead*, nextEmit*: uint64
    blocksProcessed*: uint64
    blobTxsMatched*: uint64
    blobsFetched*: uint64
    eventsPublished*: uint64
    decodeErrors*: uint64
    fetchErrors*: uint64
    reorgs*: uint64

  BlockOutcome = object
    generation: uint64
    number: uint64
    hash, parentHash: Hash32
    events: seq[(Feed, FeedEvent)]

  ProcessError = object
    notYet: bool
    msg: string

  WorkItem = tuple[generation, number: uint64]

  Relayer* = ref object
    cfg: RelayerConfig
    el: ElClient
    beacon: BeaconClient
    index: AdapterIndex
    hub: FeedHub
    started: bool
    generation: uint64
    nextSchedule: uint64
    reorder: ReorderBuffer[BlockOutcome]
    recent: Deque[(uint64, Hash32)]
    work: AsyncQueue[WorkItem]
    results: AsyncQueue[BlockOutcome]
    wake: AsyncEvent
    loops: seq[FutureBase]
    stats*: RelayerStats

proc new*(
    T: type Relayer, cfg: RelayerConfig, el: ElClient, beacon: BeaconClient,
    index: AdapterIndex, hub: FeedHub): T =
  doAssert cfg.workers > 0 and cfg.windowPerWorker > 0
  T(cfg: cfg, el: el, beacon: beacon, index: index, hub: hub,
    reorder: ReorderBuffer[BlockOutcome].init(0),
    recent: initDeque[(uint64, Hash32)](RecentHashes),
    work: newAsyncQueue[WorkItem](),
    results: newAsyncQueue[BlockOutcome](),
    wake: newAsyncEvent())

func window(r: Relayer): uint64 =
  uint64(r.cfg.workers * r.cfg.windowPerWorker)

# Block processing -------------------------------------------------------------

proc processBlock(
    r: Relayer, generation, number: uint64
): Future[Result[BlockOutcome, ProcessError]] {.async: (raises: [CancelledError]).} =
  let maybeBlock = (await r.el.getBlock(number)).valueOr:
    return err(ProcessError(msg: "el: " & error))
  if maybeBlock.isNone:
    return err(ProcessError(notYet: true, msg: "block not available yet"))
  let blk = maybeBlock.get

  var outcome = BlockOutcome(
    generation: generation, number: number,
    hash: blk.hash, parentHash: blk.parentHash)

  # Cheap path: most blocks have no tx for any configured L2.
  var matches: seq[(BlobTx, seq[L2Adapter])]
  for tx in blk.blobTxs:
    let adapters = r.index.matching(tx)
    if adapters.len > 0:
      matches.add (tx, adapters)
  if matches.len == 0:
    return ok outcome

  let slot = r.beacon.slotAt(blk.timestamp).valueOr:
    return err(ProcessError(msg: "slot: " & error))

  var wanted: seq[WantedBlob]
  for (tx, _) in matches:
    for i, vh in tx.versionedHashes:
      wanted.add WantedBlob(index: tx.firstBlobIndex + i, versionedHash: vh)

  let blobs = (await r.beacon.fetchBlobs(slot, wanted)).valueOr:
    return err(ProcessError(msg: "beacon slot " & $slot & ": " & error))
  # Blob indices are increasing in tx order, so `wanted` is already sorted
  # and `blobs` lines up with it; index it by blob index for the tx split.
  var byIndex = initTable[int, BlobRef](blobs.len)
  for i, w in wanted:
    byIndex[w.index] = blobs[i]
  r.stats.blobsFetched += uint64(blobs.len)

  for (tx, adapters) in matches:
    var txBlobs = newSeqOfCap[BlobRef](tx.versionedHashes.len)
    for i in 0 ..< tx.versionedHashes.len:
      txBlobs.add byIndex.getOrDefault(tx.firstBlobIndex + i)
    inc r.stats.blobTxsMatched

    for a in adapters:
      let feed = r.hub.getFeed(a.name).valueOr:
        continue
      let ev = FeedEvent(
        kind: FeedEventKind.Batch,
        l1Number: number, l1Hash: blk.hash, l1Timestamp: blk.timestamp,
        slot: slot, txHash: tx.hash, sender: tx.sender,
        versionedHashes: tx.versionedHashes)
      let decoded = a.decode(txBlobs)
      if decoded.isOk:
        ev.payloads = decoded.get
      else:
        ev.decodeError = decoded.error
        inc r.stats.decodeErrors
        warn "Blob decode failed", feed = a.name, l1Number = number,
          tx = $tx.hash, error = decoded.error
      # Keep raw blobs on decode failure regardless, so consumers can inspect.
      if r.cfg.retainBlobs or decoded.isErr:
        ev.blobs = txBlobs
      outcome.events.add (feed, ev)
  ok outcome

proc processWithRetry(
    r: Relayer, generation, number: uint64
): Future[Opt[BlockOutcome]] {.async: (raises: [CancelledError]).} =
  var delay = 250.milliseconds
  while generation == r.generation:
    let res = await r.processBlock(generation, number)
    if res.isOk:
      return Opt.some(res.get)
    if res.error.notYet:
      await sleepAsync(r.cfg.pollInterval)
      continue
    inc r.stats.fetchErrors
    warn "Block processing failed, retrying", number, error = res.error.msg,
      retryIn = delay
    await sleepAsync(delay)
    delay = min(delay * 2, MaxRetryDelay)
  Opt.none(BlockOutcome)

# Emission & reorgs -------------------------------------------------------------

proc rewind(r: Relayer, toNext: uint64) =
  inc r.generation
  r.reorder.reset(toNext)
  r.nextSchedule = toNext
  r.work.clear()
  r.wake.fire()

proc emit(r: Relayer, o: BlockOutcome): bool =
  ## Returns false if a reorg was detected (and handled) instead.
  if r.recent.len > 0:
    let (lastNum, lastHash) = r.recent.peekLast
    if lastNum + 1 == o.number and lastHash != o.parentHash:
      discard r.recent.popLast()
      let rewindTo = lastNum - 1
      inc r.stats.reorgs
      warn "L1 reorg detected, rewinding", at = o.number,
        expectedParent = $lastHash, gotParent = $o.parentHash, rewindTo
      r.hub.publishAll(proc(): FeedEvent =
        FeedEvent(kind: FeedEventKind.Reorg, rewindTo: rewindTo))
      r.rewind(lastNum)
      return false

  for (feed, ev) in o.events:
    feed.publish(ev)
    inc r.stats.eventsPublished
    info "Published batch", feed = feed.name, seqNo = ev.seqNo,
      l1Number = o.number, tx = $ev.txHash, blobs = ev.versionedHashes.len,
      payloadBytes = ev.payloadBytes

  if r.recent.len >= RecentHashes:
    discard r.recent.popFirst()
  r.recent.addLast((o.number, o.hash))
  inc r.stats.blocksProcessed
  true

# Loops -----------------------------------------------------------------------

proc headLoop(r: Relayer) {.async: (raises: [CancelledError]).} =
  while true:
    let head = await r.el.blockNumber()
    if head.isOk:
      r.stats.elHead = head.get
      r.stats.safeHead =
        if head.get > r.cfg.confirmations: head.get - r.cfg.confirmations
        else: 0
      if not r.started:
        let start = r.cfg.startBlock.get(r.stats.safeHead)
        r.reorder.reset(start)
        r.nextSchedule = start
        r.started = true
        info "Relayer starting", startBlock = start, elHead = head.get,
          confirmations = r.cfg.confirmations, workers = r.cfg.workers
      r.wake.fire()
    else:
      warn "Failed to fetch EL head", error = head.error
    await sleepAsync(r.cfg.pollInterval)

proc schedulerLoop(r: Relayer) {.async: (raises: [CancelledError]).} =
  while true:
    await r.wake.wait()
    r.wake.clear()
    if not r.started: continue
    while r.nextSchedule <= r.stats.safeHead and
        r.nextSchedule < r.reorder.next + r.window:
      try: r.work.addLastNoWait((r.generation, r.nextSchedule))
      except AsyncQueueFullError: raiseAssert "unbounded queue"
      inc r.nextSchedule

proc workerLoop(r: Relayer) {.async: (raises: [CancelledError]).} =
  while true:
    let item = await r.work.popFirst()
    if item.generation != r.generation:
      continue
    let outcome = await r.processWithRetry(item.generation, item.number)
    if outcome.isSome:
      try: r.results.addLastNoWait(outcome.get)
      except AsyncQueueFullError: raiseAssert "unbounded queue"

proc emitterLoop(r: Relayer) {.async: (raises: [CancelledError]).} =
  while true:
    let o = await r.results.popFirst()
    if o.generation != r.generation:
      continue  # stale: produced before a reorg rewind
    discard r.reorder.push(o.number, o)
    while true:
      let ready = r.reorder.popReady().valueOr:
        break
      if not r.emit(ready):
        break
    r.stats.nextEmit = r.reorder.next
    r.wake.fire()  # window moved, let the scheduler top up

proc start*(r: Relayer) =
  r.loops.add r.headLoop()
  r.loops.add r.schedulerLoop()
  r.loops.add r.emitterLoop()
  for _ in 0 ..< r.cfg.workers:
    r.loops.add r.workerLoop()

proc stop*(r: Relayer) {.async: (raises: []).} =
  var pending: seq[Future[void].Raising([])]
  for f in r.loops:
    pending.add f.cancelAndWait()
  await noCancel allFutures(pending)
  r.loops.setLen(0)
