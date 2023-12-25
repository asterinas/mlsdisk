# SPDX-License-Identifier: GPL-2.0

KDIR ?=
ifndef KDIR
$(error The kernel source directory (KDIR) is not defined)
endif
ifeq ($(shell test -d $(KDIR) && echo true),)
$(error Invalid path specified in KDIR)
endif
include $(KDIR)/scripts/Kbuild.include

CC := clang
PWD := $(shell pwd)
DEPS_DIR := $(PWD)/target/target/release/deps
MOD_DIR := $(PWD)/dm-sworndisk

# A particular version of the Rust compiler is required.
current = $(shell rustc --version | cut -d' ' -f2)
expected = $(shell $(KDIR)/scripts/min-tool-version.sh rustc)
ifneq ($(current),$(expected))
$(warning $(shell rustup override set $(expected)))
endif

# Allows the usage of unstable features in stable compilers.
export RUSTC_BOOTSTRAP := 1

rust_common_flags := \
	-Zbinary_dep_depinfo=y \
	-Csymbol-mangling-version=v0 \
	-Cdebug-assertions=$(if $(CONFIG_RUST_DEBUG_ASSERTIONS),y,n) \
	-Coverflow-checks=$(if $(CONFIG_RUST_OVERFLOW_CHECKS),y,n) \
	-Cforce-frame-pointers=y

rust_opt_flags := \
	-Cembed-bitcode=n -Clto=n -Copt-level=2 \
	-Cforce-unwind-tables=n -Ccodegen-units=1 \
	-Zfunction-sections=n

KBUILD_RUSTFLAGS := $(rust_common_flags) $(rust_opt_flags) \
	-Zcrate-attr=no_std \
	-Ztune-cpu=generic -Cno-redzone=y \
	-Cpanic=abort -Crelocation-model=static -Ccode-model=kernel \
	-Ctarget-feature=-sse,-sse2,-sse3,-ssse3,-sse4.1,-sse4.2,-avx,-avx2 \
	--extern core --extern alloc --extern kernel \
	-L $(KDIR)/rust \
	--emit=dep-info,obj \
	--cfg no_fp_fmt_parse \
	--cfg no_global_oom_handling \
	--cfg MODULE

KBUILD_GENERATED_CFG := @$(KDIR)/include/generated/rustc_cfg

CARGO_BUILD_RUSTFLAGS := $(KBUILD_RUSTFLAGS) $(KBUILD_GENERATED_CFG)

TARGET := $(KDIR)/scripts/target.json

export CARGO_BUILD_RUSTFLAGS

CONFIG_DM_SWORNDISK 		:= m
obj-$(CONFIG_DM_SWORNDISK)	+= dm_sworndisk.o

default: dm_sworndisk.o
	@make -C $(KDIR) M=$(PWD) modules

clean:
	@make -C $(KDIR) M=$(PWD) clean
	@cargo clean

fmt:
	@cd $(MOD_DIR) && cargo fmt

# Use [cargo-patch](https://crates.io/crates/cargo-patch)
# to patch dependencies using patch files.
deps: FORCE
ifeq ($(wildcard $(MOD_DIR)/target/patch),)
	@cd $(MOD_DIR) && cargo patch
endif

cmd_cargo_build = RUST_MODFILE=$(MOD_DIR) cargo build \
	--target $(TARGET) \
	--target-dir $(PWD)/target \
	--manifest-path $(MOD_DIR)/Cargo.toml \
	--release --verbose

dm_sworndisk.o: deps $(MOD_DIR)/src/lib.rs FORCE
	$(call if_changed,cargo_build)
	@ld.lld -r $$(ls -d $(DEPS_DIR)/* | grep '.*[\.o]$$') -o $(PWD)/$@

.PHONY: FORCE
FORCE:

