// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2023 Rust-for-Linux
// Copyright (c) 2023 Ant Group CO., Ltd.
/*
 * Non-trivial C macros cannot be used in Rust. Similarly, inlined C functions
 * cannot be called either. This file explicitly creates functions ("helpers")
 * that wrap those so that they can be called from Rust.
 *
 * Even though Rust kernel modules should never use directly the bindings, some
 * of these helpers need to be exported because Rust generics and inlined
 * functions may not get their code generated in the crate where they are
 * defined. Other helpers, called from non-inline functions, may not be
 * exported, in principle. However, in general, the Rust compiler does not
 * guarantee codegen will be performed for a non-inline function either.
 * Therefore, this file exports all the helpers. In the future, this may be
 * revisited to reduce the number of exports after the compiler is informed
 * about the places codegen is required.
 *
 * All symbols are exported as GPL-only to guarantee no GPL-only feature is
 * accidentally exposed.
 */

#include <linux/atomic.h>
#include <linux/refcount.h>

refcount_t helper_REFCOUNT_INIT(int n)
{
	return (refcount_t)REFCOUNT_INIT(n);
}

void helper_refcount_inc(refcount_t *r)
{
	refcount_inc(r);
}

int helper_atomic_fetch_dec_acquire(atomic_t *v)
{
	return atomic_fetch_dec_acquire(v);
}

int helper_atomic_read_acquire(const atomic_t *v)
{
	return atomic_read_acquire(v);
}

int helper_atomic_fetch_add_release(int i, atomic_t *v)
{
	return atomic_fetch_add_release(i, v);
}
