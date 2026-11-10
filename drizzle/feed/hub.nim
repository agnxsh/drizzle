# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Per-feed fan-out with replay.
##
## `publish` never awaits. Each subscriber has a bounded queue; when it is
## full the subscriber is cut off with a `Lagged` event instead of slowing
## down the pipeline or other subscribers. Recent events live in a ring
## buffer so reconnecting clients can resume with `from = lastSeq + 1`.

import
  std/[deques, tables],
  chronos, chronicles,
  ./events, ../l2/adapter

export events, deques, tables

type
  Subscriber* = ref object
    queue*: AsyncQueue[FeedEvent]
    closed*: bool

  FeedStats* = object
    published*: uint64
    payloadBytes*: uint64
    laggedDrops*: uint64

  Feed* = ref object
    adapter*: L2Adapter
    ring: Deque[FeedEvent]
    ringSize: int
    queueSize: int
    nextSeq: uint64
    subscribers: seq[Subscriber]
    stats*: FeedStats

  FeedHub* = ref object
    feeds*: OrderedTable[string, Feed]

  SubscribeError* {.pure.} = enum
    TooOld   ## requested seq has been evicted from the ring

func name*(f: Feed): string = f.adapter.name
func subscriberCount*(f: Feed): int = f.subscribers.len
func headSeq*(f: Feed): uint64 = f.nextSeq - 1
func oldestSeq*(f: Feed): uint64 =
  if f.ring.len == 0: f.nextSeq else: f.ring.peekFirst.seqNo

proc new*(T: type Feed, adapter: L2Adapter, ringSize, queueSize: int): T =
  doAssert ringSize > 0 and queueSize > 0
  T(adapter: adapter, ring: initDeque[FeedEvent](ringSize),
    ringSize: ringSize, queueSize: queueSize, nextSeq: 1)

proc new*(T: type FeedHub, adapters: openArray[L2Adapter],
          ringSize, queueSize: int): T =
  let hub = T()
  for a in adapters:
    hub.feeds[a.name] = Feed.new(a, ringSize, queueSize)
  hub

proc getFeed*(hub: FeedHub, name: string): Opt[Feed] =
  let f = hub.feeds.getOrDefault(name)
  if f.isNil: Opt.none(Feed) else: Opt.some(f)

proc drop(f: Feed, sub: Subscriber) =
  sub.closed = true
  sub.queue.clear()
  # Room is guaranteed after clear(); the reader wakes up and sees Lagged.
  try: sub.queue.addLastNoWait(FeedEvent(kind: FeedEventKind.Lagged, feed: f.name))
  except AsyncQueueFullError: discard
  inc f.stats.laggedDrops

proc publish*(f: Feed, ev: FeedEvent) =
  ev.seqNo = f.nextSeq
  ev.feed = f.adapter.name
  inc f.nextSeq
  inc f.stats.published
  f.stats.payloadBytes += uint64(ev.payloadBytes)

  if f.ring.len >= f.ringSize:
    discard f.ring.popFirst()
  f.ring.addLast(ev)

  var i = 0
  while i < f.subscribers.len:
    let sub = f.subscribers[i]
    if sub.closed:
      f.subscribers.del(i)
      continue
    if sub.queue.full:
      debug "Dropping lagging subscriber", feed = f.name, seqNo = ev.seqNo
      f.drop(sub)
      f.subscribers.del(i)
      continue
    try: sub.queue.addLastNoWait(ev)
    except AsyncQueueFullError: raiseAssert "checked not full"
    inc i

proc subscribe*(
    f: Feed, fromSeq: Opt[uint64]
): Result[Subscriber, SubscribeError] =
  ## Without `fromSeq`, the stream starts at the next published event.
  ## With it, ring events >= fromSeq are replayed first.
  var replay: seq[FeedEvent]
  if fromSeq.isSome:
    let start = fromSeq.get
    if start < f.oldestSeq and start < f.nextSeq:
      return err(SubscribeError.TooOld)
    for ev in f.ring:
      if ev.seqNo >= start: replay.add ev
  # Size the queue so the replay itself can't trip the lag check.
  let sub = Subscriber(
    queue: newAsyncQueue[FeedEvent](f.queueSize + replay.len))
  for ev in replay:
    try: sub.queue.addLastNoWait(ev)
    except AsyncQueueFullError: raiseAssert "sized for replay"
  f.subscribers.add sub
  ok sub

proc unsubscribe*(f: Feed, sub: Subscriber) =
  sub.closed = true
  let i = f.subscribers.find(sub)
  if i >= 0: f.subscribers.del(i)

func eventsSince*(f: Feed, fromSeq: uint64, limit: int): seq[FeedEvent] =
  for ev in f.ring:
    if result.len >= limit: break
    if ev.seqNo >= fromSeq: result.add ev

proc publishAll*(hub: FeedHub, mkEvent: proc(): FeedEvent {.gcsafe, raises: [].}) =
  ## Broadcast (used for reorg notices): a fresh event object per feed,
  ## since seqNo and feed name are per-feed.
  for f in hub.feeds.values:
    f.publish(mkEvent())
