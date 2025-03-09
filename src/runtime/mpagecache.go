// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"runtime/internal/sys"
	"unsafe"
)

const pageCachePages = 8 * unsafe.Sizeof(pageCache{}.cache)

// pageCache represents a per-p cache of pages the allocator can
// allocate from without a lock. More specifically, it represents
// a pageCachePages*pageSize chunk of memory with 0 or more free
// pages in it.
/*
	pageCache 是 Go 运行时内存分配器的核心组件之一，用于实现无锁的高效内存页分配。
	设计目标：
	1. 无锁分配：每个 P（逻辑处理器）拥有独立的 pageCache，分配时无需全局锁。
	2. 快速分配路径：通过位图（cache）实现 O(1) 时间复杂度的页分配。
	3. 物理内存回收：通过 scav 位图跟踪可释放的物理内存页，支持按需返还给操作系统。
	4. 缓存填充：从全局堆补充：当 pageCache 为空时，从全局 mheap 申请新的内存块。
	5. 操作系统交互：通过 madvise(MADV_FREE) 通知内核可回收物理内存，但保留虚拟地址空间。
*/
type pageCache struct {
	// 内存块的基地址，指向连续内存区域的起始位置
	base uintptr // base address of the chunk
	// 位图管理 64 页（默认配置）的空闲状态，1 表示对应页可用
	cache uint64 // 64-bit bitmap representing free pages (1 means free)
	// 位图记录已释放物理内存的页（通过 madvise 通知操作系统回收物理内存）
	scav uint64 // 64-bit bitmap representing scavenged pages (1 means scavenged)
}

// empty returns true if the pageCache has any free pages, and false
// otherwise.
func (c *pageCache) empty() bool {
	return c.cache == 0
}

// alloc allocates npages from the page cache and is the main entry
// point for allocation.
//
// Returns a base address and the amount of scavenged memory in the
// allocated region in bytes.
//
// Returns a base address of zero on failure, in which case the
// amount of scavenged memory should be ignored.
func (c *pageCache) alloc(npages uintptr) (uintptr, uintptr) {
	if c.cache == 0 {
		return 0, 0
	}
	if npages == 1 {
		i := uintptr(sys.TrailingZeros64(c.cache))
		scav := (c.scav >> i) & 1
		c.cache &^= 1 << i // set bit to mark in-use
		c.scav &^= 1 << i  // clear bit to mark unscavenged
		return c.base + i*pageSize, uintptr(scav) * pageSize
	}
	return c.allocN(npages)
}

// allocN is a helper which attempts to allocate npages worth of pages
// from the cache. It represents the general case for allocating from
// the page cache.
//
// Returns a base address and the amount of scavenged memory in the
// allocated region in bytes.
func (c *pageCache) allocN(npages uintptr) (uintptr, uintptr) {
	i := findBitRange64(c.cache, uint(npages))
	if i >= 64 {
		return 0, 0
	}
	mask := ((uint64(1) << npages) - 1) << i
	scav := sys.OnesCount64(c.scav & mask)
	c.cache &^= mask // mark in-use bits
	c.scav &^= mask  // clear scavenged bits
	return c.base + uintptr(i*pageSize), uintptr(scav) * pageSize
}

// flush empties out unallocated free pages in the given cache
// into s. Then, it clears the cache, such that empty returns
// true.
//
// s.mheapLock must be held or the world must be stopped.
func (c *pageCache) flush(s *pageAlloc) {
	if c.empty() {
		return
	}
	ci := chunkIndex(c.base)
	pi := chunkPageIndex(c.base)

	// This method is called very infrequently, so just do the
	// slower, safer thing by iterating over each bit individually.
	for i := uint(0); i < 64; i++ {
		if c.cache&(1<<i) != 0 {
			s.chunkOf(ci).free1(pi + i)
		}
		if c.scav&(1<<i) != 0 {
			s.chunkOf(ci).scavenged.setRange(pi+i, 1)
		}
	}
	// Since this is a lot like a free, we need to make sure
	// we update the searchAddr just like free does.
	if b := (offAddr{c.base}); b.lessThan(s.searchAddr) {
		s.searchAddr = b
	}
	s.update(c.base, pageCachePages, false, false)
	*c = pageCache{}
}

// allocToCache acquires a pageCachePages-aligned chunk of free pages which
// may not be contiguous, and returns a pageCache structure which owns the
// chunk.
//
// s.mheapLock must be held.
/*
	功能：从全局页分配器 (pageAlloc) 中分配一个 64 页对齐的内存块（可能不连续），返回 pageCache 结构。
	调用条件：需持有 mheapLock，确保线程安全。
*/
func (s *pageAlloc) allocToCache() pageCache {
	// If the searchAddr refers to a region which has a higher address than
	// any known chunk, then we know we're out of memory.
	if chunkIndex(s.searchAddr.addr()) >= s.end {
		return pageCache{}
	}
	c := pageCache{}
	ci := chunkIndex(s.searchAddr.addr()) // chunk index
	if s.summary[len(s.summary)-1][ci] != 0 {
		// Fast path: there's free pages at or near the searchAddr address.
		chunk := s.chunkOf(ci)
		j, _ := chunk.find(1, chunkPageIndex(s.searchAddr.addr()))
		if j == ^uint(0) {
			throw("bad summary data")
		}
		c = pageCache{
			base:  chunkBase(ci) + alignDown(uintptr(j), 64)*pageSize,
			cache: ^chunk.pages64(j),
			scav:  chunk.scavenged.block64(j),
		}
	} else {
		// Slow path: the searchAddr address had nothing there, so go find
		// the first free page the slow way.
		addr, _ := s.find(1)
		if addr == 0 {
			// We failed to find adequate free space, so mark the searchAddr as OoM
			// and return an empty pageCache.
			s.searchAddr = maxSearchAddr
			return pageCache{}
		}
		ci := chunkIndex(addr)
		chunk := s.chunkOf(ci)
		c = pageCache{
			base:  alignDown(addr, 64*pageSize),
			cache: ^chunk.pages64(chunkPageIndex(addr)),
			scav:  chunk.scavenged.block64(chunkPageIndex(addr)),
		}
	}

	// Set the bits as allocated and clear the scavenged bits.
	s.allocRange(c.base, pageCachePages)

	// Update as an allocation, but note that it's not contiguous.
	s.update(c.base, pageCachePages, false, true)

	// Set the search address to the last page represented by the cache.
	// Since all of the pages in this block are going to the cache, and we
	// searched for the first free page, we can confidently start at the
	// next page.
	//
	// However, s.searchAddr is not allowed to point into unmapped heap memory
	// unless it is maxSearchAddr, so make it the last page as opposed to
	// the page after.
	s.searchAddr = offAddr{c.base + pageSize*(pageCachePages-1)}
	return c
}
