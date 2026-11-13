# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Built-in adapter kinds. Importing this module registers them.
##
## To add an L2 family: write `drizzle/l2/<family>_codec.nim` with a pure
## decode function, wrap it in a `DecodeProc` here (or in your own module)
## and call `registerAdapterKind`.

import ./[adapter, opstack_codec, arbitrum_codec]

export adapter

func decodeOpStack(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string] =
  ## One payload per blob: each OP blob is an independent frame stream.
  var res = newSeqOfCap[seq[byte]](blobs.len)
  for i, b in blobs:
    let data = decodeOpBlob(b[]).valueOr:
      return err("blob " & $i & ": " & $error)
    res.add data
  ok res

func decodeArbitrum(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string] =
  ## One payload per tx: Nitro spreads one RLP string across all blobs.
  let data = decodeArbitrumBlobs(blobs).valueOr:
    return err($error)
  ok @[data]

func decodeRaw(blobs: openArray[BlobRef]): Result[seq[seq[byte]], string] =
  ## No decoding; consumers subscribe with `?blobs=1` to get raw blobs.
  ok newSeq[seq[byte]]()

registerAdapterKind("opstack", decodeOpStack)
# Nitro's inbox reader takes batches from SequencerBatchDelivered events, so
# a reverted addSequencerL2BatchFromBlobs is not a batch.
registerAdapterKind("arbitrum", decodeArbitrum, requireSuccess = true)
registerAdapterKind("raw", decodeRaw)
