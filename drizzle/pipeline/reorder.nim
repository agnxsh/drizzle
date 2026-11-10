# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Turns out-of-order completions from parallel workers back into a strictly
## sequential stream keyed by block number.

import
  std/tables,
  results

export results

type
  ReorderBuffer*[T] = object
    next*: uint64              ## the only key `popReady` will release
    pending: Table[uint64, T]

func init*[T](_: type ReorderBuffer[T], next: uint64): ReorderBuffer[T] =
  ReorderBuffer[T](next: next)

func len*[T](b: ReorderBuffer[T]): int = b.pending.len

func push*[T](b: var ReorderBuffer[T], key: uint64, v: sink T): bool =
  ## False when `key` was already released or is a duplicate.
  if key < b.next or key in b.pending:
    return false
  b.pending[key] = v
  true

func popReady*[T](b: var ReorderBuffer[T]): Opt[T] =
  var v: T
  if b.pending.pop(b.next, v):
    inc b.next
    Opt.some(v)
  else:
    Opt.none(T)

func reset*[T](b: var ReorderBuffer[T], next: uint64) =
  b.pending.clear()
  b.next = next
