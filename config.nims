# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

import std/os

# Two build paths share this file:
#   * Makefile (primary): dependencies come from the `vendor/` git submodules,
#     the nimble package cache is ignored so builds are hermetic.
#   * nimble (secondary): nimble passes `-d:NimblePkgVersion=...` and manages
#     the search path itself, so we leave paths alone.
if not defined(NimblePkgVersion) and dirExists(thisDir() / "vendor"):
  switch("noNimblePath")
  for kind, dir in walkDir(thisDir() / "vendor"):
    if kind == pcDir and dir.lastPathPart != "Nim":
      switch("path", dir)

# Nimbus-flavoured defaults
switch("styleCheck", "usages")
switch("styleCheck", "error")
switch("define", "chronicles_line_numbers")
switch("define", "chronicles_sinks=textlines[stdout]")
switch("define", "chronicles_runtime_filtering=on")
switch("warning", "ObservableStores:off")
switch("warningAsError", "BareExcept:on")
switch("hint", "XDeclaredButNotUsed:off")
--mm:orc
