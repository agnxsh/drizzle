# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Opt-in blob verification (`--verify-kzg`): recompute each blob's KZG
## commitment and check it hashes to the versioned hash from the L1 tx.
## That binds the blob to the transaction, so a faulty or malicious beacon
## node can't feed us other data.
##
## The `/blobs` endpoint returns no commitments or proofs, so the commitment
## is recomputed (an MSM over the blob, ~4 ms per blob on one core). That is
## far above mainnet's live rate but caps backfill at a few hundred blobs/s,
## which is why it's off by default.
##
## Uses c-kzg-4844 via nim-kzg4844 with the embedded Ethereum trusted setup.
## Spec: https://github.com/ethereum/consensus-specs/blob/master/specs/deneb/polynomial-commitments.md#blob_to_kzg_commitment

import
  kzg4844/kzg,
  ./eip4844

export results

proc loadKzgSetup*(): Result[void, string] =
  ## Load the trusted setup now (~1.5 s) instead of on the first blob.
  var zero: KzgBlob
  discard ? blobToKzgCommitment(zero)
  ok()

proc verifyBlob*(blob: Blob, expected: VersionedHash): Result[void, string] =
  static: doAssert sizeof(KzgBlob) == sizeof(Blob)
  let commitment = blobToKzgCommitment(cast[ptr KzgBlob](unsafeAddr blob[0])[]).valueOr:
    # c-kzg rejects field elements >= the BLS modulus.
    return err("not a valid blob: " & error)
  if kzgToVersionedHash(commitment.bytes) != expected:
    return err("KZG commitment does not match versioned hash " & $expected)
  ok()
