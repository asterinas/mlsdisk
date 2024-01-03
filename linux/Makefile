# SPDX-License-Identifier: GPL-2.0

KDIR ?=
ifndef KDIR
$(error The kernel source directory (KDIR) is not defined)
endif
ifeq ($(shell test -d $(KDIR) && echo true),)
$(error Invalid path specified in KDIR)
endif
include $(KDIR)/scripts/Kbuild.include
TARGET := $(KDIR)/scripts/target.json

CC	:= clang
PWD	:= $(shell pwd)
MOD_DIR	:= $(PWD)/dm-sworndisk
BINDING_DIR	:= $(PWD)/bindings
TARGET_DIR	:= $(PWD)/target
DEPS_DIR	:= $(TARGET_DIR)/target/release/deps

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

# Try NOT to modify UPPERCASE variables as much as possible, user flags should
# add to `rust_common_flags` or `rust_opt_flags`.
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

export CARGO_BUILD_RUSTFLAGS

CONFIG_DM_SWORNDISK 		:= m
obj-$(CONFIG_DM_SWORNDISK)	+= dm_sworndisk.o

default: dm_sworndisk.o
	@make -C $(KDIR) M=$(PWD) modules

clean:
	@make -C $(KDIR) M=$(PWD) clean
	@rm -r $(TARGET_DIR)

fmt:
	@cd $(MOD_DIR) && cargo fmt
	@cd $(BINDING_DIR) && cargo fmt

# Use [cargo-patch](https://crates.io/crates/cargo-patch)
# to patch dependencies using patch files.
deps: FORCE
ifeq ($(wildcard $(MOD_DIR)/target/patch),)
	@cd $(MOD_DIR) && cargo patch
endif

bindgen_c_flags := -D__KERNEL__ \
	-I $(KDIR)/arch/x86/include \
	-I $(KDIR)/arch/x86/include/generated \
	-I $(KDIR)/include \
	-I $(KDIR)/arch/x86/include/uapi \
	-I $(KDIR)/arch/x86/include/generated/uapi \
	-I $(KDIR)/include/uapi \
	-I $(KDIR)/include/generated/uapi \
	-include $(KDIR)/include/linux/compiler-version.h \
	-include $(KDIR)/include/linux/kconfig.h \
	-include $(KDIR)/include/linux/compiler_types.h

cmd_bindgen = bindgen $< $(bindgen_target_flags) \
	--use-core --with-derive-default --ctypes-prefix core::ffi \
	--no-layout-tests --no-debug '.*' \
	-o $@ -- $(bindgen_c_flags) -DMODULE \
	$(bindgen_target_cflags) $(bindgen_target_extra)

$(BINDING_DIR)/src/bindings_generated.rs: private bindgen_target_flags = \
	$(shell grep -v '^#\|^$$' $(KDIR)/rust/bindgen_parameters)
$(BINDING_DIR)/src/bindings_generated.rs: $(BINDING_DIR)/src/bindings.h \
	$(BINDING_DIR)/src/bindings_helpers_generated.rs
	$(call if_changed,bindgen)

$(BINDING_DIR)/src/bindings_helpers_generated.rs: private bindgen_target_flags = \
    --blocklist-type '.*' --allowlist-var '' \
    --allowlist-function 'helper_.*'
$(BINDING_DIR)/src/bindings_helpers_generated.rs: private bindgen_target_extra = ; \
    sed -Ei 's/pub fn helper_([a-zA-Z0-9_]*)/#[link_name="helper_\1"]\n    pub fn \1/g' $@
$(BINDING_DIR)/src/bindings_helpers_generated.rs: $(BINDING_DIR)/src/helpers.c FORCE
	$(call if_changed,bindgen)

cmd_cargo_build = RUST_MODFILE=$(MOD_DIR) cargo build \
	--target $(TARGET) \
	--target-dir $(TARGET_DIR) \
	--manifest-path $(MOD_DIR)/Cargo.toml \
	--release --verbose

# This is needed when compiles the `bindings/src/helpers.c`.
CFLAGS += -nostdinc $(bindgen_c_flags) \
	--target=x86_64-linux-gnu -std=gnu11 -fintegrated-as \
	-fno-common -fno-PIE -fno-strict-aliasing \
	-mno-sse -mno-mmx -mno-sse2 -mno-3dnow -mno-avx \
	-fcf-protection=none -m64 -mno-80387 -mstack-alignment=8 \
	-mtune=generic -mno-red-zone -mcmodel=kernel \
	-mretpoline-external-thunk

dm_sworndisk.o: deps $(MOD_DIR)/src/lib.rs $(BINDING_DIR)/src/helpers.o \
	$(BINDING_DIR)/src/bindings_generated.rs FORCE
	$(call if_changed,cargo_build)
	@ld.lld -r $$(ls -d $(DEPS_DIR)/* | grep '.*[\.o]$$') \
	$(BINDING_DIR)/src/helpers.o -o $(PWD)/$@

.PHONY: FORCE
FORCE:

