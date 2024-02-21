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

#include <crypto/aead.h>
#include <crypto/skcipher.h>
#include <linux/atomic.h>
#include <linux/bio.h>
#include <linux/refcount.h>
#include <linux/sched/task.h>
#include <linux/scatterlist.h>

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

struct task_struct *helper_get_task_struct(struct task_struct *t)
{
	return get_task_struct(t);
}

void helper_put_task_struct(struct task_struct *t)
{
	put_task_struct(t);
}

void helper_bio_get(struct bio *bio)
{
	bio_get(bio);
}

void helper_bio_set_dev(struct bio *bio, struct block_device *bdev)
{
	bio_set_dev(bio, bdev);
}

bool helper_bio_has_data(struct bio *bio)
{
	return bio_has_data(bio);
}

struct page *helper_virt_to_page(void *addr)
{
	return virt_to_page(addr);
}

void *helper_page_to_virt(struct page *page)
{
	return page_to_virt(page);
}

void helper_sg_set_buf(struct scatterlist *sg, const void *buf,
				unsigned int buflen)
{
	sg_set_buf(sg, buf, buflen);
}

struct aead_request *helper_aead_request_alloc(struct crypto_aead *tfm,
				gfp_t gfp)
{
	return aead_request_alloc(tfm, gfp);
}

struct skcipher_request *helper_skcipher_request_alloc(
				struct crypto_skcipher *tfm, gfp_t gfp)
{
	return skcipher_request_alloc(tfm, gfp);
}
