ENABLE_FEATURES ?= default

# Disable portable on MacOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
ROCKSDB_SYS_PORTABLE=0
endif

# Build portable binary by default unless disable explicitly
ifneq ($(ROCKSDB_SYS_PORTABLE),0)
ENABLE_FEATURES += portable
endif

default:
	@cargo build --features "${ENABLE_FEATURES}" --release
	@mkdir bin
	@cp -f $(CURDIR)/target/release/rocksdb_consistency_test ./bin/rocksdb_consistency_test

clean:
	@cargo clean