# drizzle - pluggable L2 blob feed relayer
# Copyright (c) 2026 Agnish Ghosh <iamagnix@gmail.com>
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# Primary build entry point. Dependencies live in `vendor/` as git submodules
# (Nimbus-style); nimble is supported as a secondary path via drizzle.nimble.
#
#   make update      - init/update all submodules (recursive)
#   make nim         - bootstrap the vendored Nim compiler (vendor/Nim)
#   make             - build build/drizzle
#   make test        - run the unit test suite
#   make bench       - codec + end-to-end throughput benchmarks
#   make NIM=nim ... - use a Nim from PATH instead (must be >= 2.2.10)

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

VENDORED_NIM := vendor/Nim/bin/nim
NIM          ?= $(VENDORED_NIM)
BUILD_DIR    := build
NIMCACHE     := nimcache

# -d:release by default; `make DEBUG=1` for a debug build.
ifeq ($(DEBUG),1)
  NIM_PARAMS ?= -d:debug --debugger:native
else
  NIM_PARAMS ?= -d:release
endif

# Chronicles log level baked in at compile time (runtime can only lower it).
LOG_LEVEL ?= DEBUG
NIM_PARAMS += -d:chronicles_log_level=$(LOG_LEVEL)

.PHONY: all drizzle test bench update nim clean clean-nim deps-check

all: drizzle

# Only bootstrap vendor/Nim when it is the compiler actually in use.
ifeq ($(NIM),$(VENDORED_NIM))
NIM_DEP := $(VENDORED_NIM)
else
NIM_DEP :=
endif

$(VENDORED_NIM):
	@if [[ ! -f vendor/Nim/koch.nim ]]; then \
	  echo "vendor/Nim missing - run 'make update' first"; exit 1; fi
	cd vendor/Nim && \
	  . ci/funs.sh && nimBuildCsourcesIfNeeded && \
	  bin/nim c --noNimblePath --skipUserCfg --skipParentCfg --hints:off koch && \
	  ./koch boot -d:release --skipUserCfg --skipParentCfg --hints:off

nim: $(VENDORED_NIM)

update:
	git submodule sync --recursive
	git submodule update --init --recursive

deps-check:
	@for d in vendor/*/; do \
	  if [[ -z "$$(ls -A $$d)" ]]; then \
	    echo "empty submodule $$d - run 'make update'"; exit 1; fi; done

drizzle: deps-check $(NIM_DEP)
	@mkdir -p $(BUILD_DIR)
	$(NIM) c $(NIM_PARAMS) --nimcache:$(NIMCACHE)/drizzle \
	  -o:$(BUILD_DIR)/drizzle drizzle/drizzle.nim

test: deps-check $(NIM_DEP)
	@mkdir -p $(BUILD_DIR)
	$(NIM) c $(NIM_PARAMS) --nimcache:$(NIMCACHE)/tests \
	  -o:$(BUILD_DIR)/all_tests -r tests/all_tests.nim

bench: deps-check $(NIM_DEP)
	@mkdir -p $(BUILD_DIR)
	$(NIM) c -d:release -d:chronicles_log_level=WARN --nimcache:$(NIMCACHE)/bench \
	  -o:$(BUILD_DIR)/bench_throughput -r bench/bench_throughput.nim

clean:
	rm -rf $(BUILD_DIR) $(NIMCACHE)

clean-nim:
	cd vendor/Nim && git clean -fdx
