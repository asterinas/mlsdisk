# Linux kernel specific crates.

## Bindings

This crate is used to generate bindings to the C side of the kernel at build time using
the [``bindgen``](https://github.com/rust-lang/rust-bindgen) tool. A particular version is required.

Install it via (note that this will download and build the tool from source)::
```shell
cd <linux_source_dir>
cargo install --locked --version $(scripts/min-tool-version.sh bindgen) bindgen-cli
```

``bindgen`` needs to find a suitable ``libclang`` in order to work. If it is
not found (or a different ``libclang`` than the one found should be used),
the process can be tweaked using the environment variables understood by
``clang-sys`` (the Rust bindings crate that ``bindgen`` uses to access
``libclang``):

* ``LLVM_CONFIG_PATH`` can be pointed to an ``llvm-config`` executable.

* Or ``LIBCLANG_PATH`` can be pointed to a ``libclang`` shared library
  or to the directory containing it.

* Or ``CLANG_PATH`` can be pointed to a ``clang`` executable.

## Dm-sworndisk

This crate implements a kernel module that enables ``sworndisk`` running in kernel as a device mapper target, based on [``rust-for-linux``](https://rust-for-linux.com/) project.

To compile this crate, you should download and build the ``rust-for-linux`` kernel first, refer to this [``document``](https://docs.kernel.org/next/rust/) for details.

Our build is based on the ``rust-next`` branch of this [``repo``](https://github.com/Rust-for-Linux/linux), checked out with commit ``b2516f7af9d238ebc391bdbdae01ac9528f1109e``.

After building the kernel successfully, you could build the kernel module with our ``make`` script:

```shell
KDIR=<linux_source_dir> make
```

To format the codes of those crate, run:

```shell
KDIR=<linux_source_dir> make fmt
```

To clean object files or kernel module files, run:

```shell
KDIR=<linux_source_dir> make clean
```

## Test the module

TODO: you should prepare a virtual machine, running the kernel with rust enabled.
