# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

## Well-known L2 feeds and the `--feed` CLI syntax.
##
##   --feed=base                              preset
##   --feed=base:opstack:0xff00..8453         custom, any sender
##   --feed=mychain:arbitrum:0xINBOX:0xA,0xB  custom, batcher allow-list
##
## Preset addresses were correct at time of writing; batchers rotate. Always
## verify against the chain's official docs (see NOTES.md section 7).

import
  std/strutils,
  ./builtin

export builtin

type
  FeedPreset* = object
    name*, kind*, inbox*: string
    batchers*: seq[string]

const Presets*: array[3, FeedPreset] = [
  FeedPreset(
    name: "base", kind: "opstack",
    inbox: "0xff00000000000000000000000000000000008453",
    batchers: @["0x5050f69a9786f081509234f1a7f4684b5e5b76c9"]),
  FeedPreset(
    name: "optimism", kind: "opstack",
    inbox: "0xff00000000000000000000000000000000000010",
    batchers: @["0x6887246668a3b87f54deb3b94ba47a6f63f32985"]),
  FeedPreset(
    name: "arbitrum-one", kind: "arbitrum",
    # SequencerInbox; the batch poster calls addSequencerL2BatchFromBlobs.
    inbox: "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6",
    batchers: @[]),
]

proc parseAddressList(s: string): Result[seq[Address], string] =
  var res: seq[Address]
  for part in s.split(','):
    let p = part.strip()
    if p.len == 0: continue
    let address = Address.fromHex(p).valueOr:
      return err("invalid address '" & p & "': " & $error)
    res.add address
  ok res

proc fromPreset(p: FeedPreset): Result[L2Adapter, string] =
  let inbox = Address.fromHex(p.inbox).valueOr:
    return err("bad preset inbox for " & p.name)
  newAdapter(p.name, p.kind, inbox, ? parseAddressList(p.batchers.join(",")))

proc parseFeedSpec*(spec: string): Result[L2Adapter, string] =
  let parts = spec.strip().split(':')
  if parts.len == 1:
    for p in Presets:
      if p.name == parts[0].toLowerAscii:
        return fromPreset(p)
    var names: seq[string]
    for p in Presets: names.add p.name
    return err("unknown preset '" & parts[0] & "', known: " & names.join(", "))
  if parts.len notin 3..4:
    return err("feed spec must be '<preset>' or " &
               "'<name>:<kind>:<inbox>[:<batcher>,...]', got '" & spec & "'")
  let inbox = Address.fromHex(parts[2]).valueOr:
    return err("invalid inbox address '" & parts[2] & "'")
  let batchers =
    if parts.len == 4: ? parseAddressList(parts[3])
    else: @[]
  newAdapter(parts[0], parts[1], inbox, batchers)
