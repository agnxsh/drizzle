# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# Secondary build path. The Makefile + `vendor/` submodules are canonical;
# keep the versions below in sync with the pinned submodule tags.

version       = "0.1.0"
author        = "Agnish Ghosh <iamagnix@gmail.com>"
description   = "High-throughput EIP-4844 blob feed relayer with pluggable L2 adapters"
license       = "Apache License 2.0 or MIT"
srcDir        = "."
bin           = @["drizzle/drizzle"]
binDir        = "build"
skipDirs      = @["tests", "vendor"]

requires "nim >= 2.2.10",
         "chronos >= 4.4.1",
         "chronicles >= 0.12.4",
         "results >= 0.5.1",
         "stew >= 0.6.5",
         "nimcrypto >= 0.7.3",
         "confutils >= 0.2.0",
         "kzg4844 >= 0.2.0",
         "unittest2 >= 0.3.0"

task test, "Run all tests":
  exec "nim c -d:NimblePkgVersion=" & version & " -r tests/all_tests.nim"
