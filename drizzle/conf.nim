# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Command line configuration.

import
  std/options,
  confutils, confutils/defs

export options, defs

const
  DrizzleVersion* = "0.1.0"

type
  DrizzleConf* = object
    logLevel* {.
      desc: "Log level: TRACE, DEBUG, INFO, NOTICE, WARN, ERROR"
      defaultValue: "INFO"
      name: "log-level" .}: string

    elUrl* {.
      desc: "Execution layer JSON-RPC URL"
      defaultValue: "http://127.0.0.1:8545"
      name: "el-url" .}: string

    beaconUrl* {.
      desc: "Beacon node REST API URL"
      defaultValue: "http://127.0.0.1:5052"
      name: "beacon-url" .}: string

    beaconApi* {.
      desc: "Blob endpoint: auto, blobs (/eth/v1/beacon/blobs) or sidecars (/eth/v1/beacon/blob_sidecars)"
      defaultValue: "auto"
      name: "beacon-api" .}: string

    feeds* {.
      desc: "Feed to relay (repeatable). Preset (base, optimism, arbitrum-one) or <name>:<kind>:<inbox>[:<batcher>,...] with kind opstack, arbitrum or raw"
      name: "feed" .}: seq[string]

    startBlock* {.
      desc: "First L1 block to process (default: current head minus confirmations)"
      name: "start-block" .}: Option[uint64]

    confirmations* {.
      desc: "Only process L1 blocks this many blocks behind head"
      defaultValue: 0
      name: "confirmations" .}: uint64

    workers* {.
      desc: "Parallel block fetchers"
      defaultValue: 16
      name: "workers" .}: int

    pollIntervalMs* {.
      desc: "EL head poll interval in milliseconds"
      defaultValue: 1000
      name: "poll-interval-ms" .}: int

    requestTimeoutMs* {.
      desc: "Upstream request timeout in milliseconds"
      defaultValue: 15000
      name: "request-timeout-ms" .}: int

    maxUpstreamConnections* {.
      desc: "Max pooled connections per upstream (-1 = unlimited)"
      defaultValue: 32
      name: "max-upstream-connections" .}: int

    retainBlobs* {.
      desc: "Keep raw blobs on events so consumers can request them with ?blobs=1"
      defaultValue: true
      name: "retain-blobs" .}: bool

    ringSize* {.
      desc: "Events kept per feed for replay"
      defaultValue: 256
      name: "ring-size" .}: int

    subscriberQueue* {.
      desc: "Events buffered per subscriber before it is dropped as lagging"
      defaultValue: 64
      name: "subscriber-queue" .}: int

    apiAddress* {.
      desc: "Listening address of the feed API"
      defaultValue: "127.0.0.1"
      name: "api-address" .}: string

    apiPort* {.
      desc: "Listening port of the feed API"
      defaultValue: 7700
      name: "api-port" .}: uint16

    apiMaxConnections* {.
      desc: "Max concurrent API connections (-1 = unlimited)"
      defaultValue: 1024
      name: "api-max-connections" .}: int

    heartbeatSecs* {.
      desc: "Idle stream heartbeat interval in seconds"
      defaultValue: 15
      name: "heartbeat-secs" .}: int
