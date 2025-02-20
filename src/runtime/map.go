// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

// This file contains the implementation of Go's map type.
//
// A map is just a hash table. The data is arranged
// into an array of buckets. Each bucket contains up to
// 8 key/elem pairs. The low-order bits of the hash are
// used to select a bucket. Each bucket contains a few
// high-order bits of each hash to distinguish the entries
// within a single bucket.
//
// If more than 8 keys hash to a bucket, we chain on
// extra buckets.
//
// When the hashtable grows, we allocate a new array
// of buckets twice as big. Buckets are incrementally
// copied from the old bucket array to the new bucket array.
//
// Map iterators walk through the array of buckets and
// return the keys in walk order (bucket #, then overflow
// chain order, then bucket index).  To maintain iteration
// semantics, we never move keys within their bucket (if
// we did, keys might be returned 0 or 2 times).  When
// growing the table, iterators remain iterating through the
// old table and must check the new table if the bucket
// they are iterating through has been moved ("evacuated")
// to the new table.

// Picking loadFactor: too large and we have lots of overflow
// buckets, too small and we waste a lot of space. I wrote
// a simple program to check some stats for different loads:
// (64-bit, 8 byte keys and elems)
//  loadFactor    %overflow  bytes/entry     hitprobe    missprobe
//        4.00         2.13        20.77         3.00         4.00
//        4.50         4.05        17.30         3.25         4.50
//        5.00         6.85        14.77         3.50         5.00
//        5.50        10.55        12.94         3.75         5.50
//        6.00        15.27        11.67         4.00         6.00
//        6.50        20.90        10.79         4.25         6.50
//        7.00        27.14        10.15         4.50         7.00
//        7.50        34.03         9.73         4.75         7.50
//        8.00        41.10         9.40         5.00         8.00
//
// %overflow   = percentage of buckets which have an overflow bucket
// bytes/entry = overhead bytes used per key/elem pair
// hitprobe    = # of entries to check when looking up a present key
// missprobe   = # of entries to check when looking up an absent key
//
// Keep in mind this data is for maximally loaded tables, i.e. just
// before the table grows. Typical tables will be somewhat less loaded.

import (
	"runtime/internal/atomic"
	"runtime/internal/math"
	"runtime/internal/sys"
	"unsafe"
)

const (
	// Maximum number of key/elem pairs a bucket can hold.
	bucketCntBits = 3
	bucketCnt     = 1 << bucketCntBits

	// Maximum average load of a bucket that triggers growth is 6.5.
	// Represent as loadFactorNum/loadFactorDen, to allow integer math.
	loadFactorNum = 13
	loadFactorDen = 2

	// Maximum key or elem size to keep inline (instead of mallocing per element).
	// Must fit in a uint8.
	// Fast versions cannot handle big elems - the cutoff size for
	// fast versions in cmd/compile/internal/gc/walk.go must be at most this elem.
	maxKeySize  = 128
	maxElemSize = 128

	// data offset should be the size of the bmap struct, but needs to be
	// aligned correctly. For amd64p32 this means 64-bit alignment
	// even though pointers are 32 bit.
	dataOffset = unsafe.Offsetof(struct {
		b bmap
		v int64
	}{}.v)

	// Possible tophash values. We reserve a few possibilities for special marks.
	// Each bucket (including its overflow buckets, if any) will have either all or none of its
	// entries in the evacuated* states (except during the evacuate() method, which only happens
	// during map writes and thus no one else can observe the map during that time).
	emptyRest      = 0 // this cell is empty, and there are no more non-empty cells at higher indexes or overflows.
	emptyOne       = 1 // this cell is empty
	evacuatedX     = 2 // key/elem is valid.  Entry has been evacuated to first half of larger table.
	evacuatedY     = 3 // same as above, but evacuated to second half of larger table.
	evacuatedEmpty = 4 // cell is empty, bucket is evacuated.
	minTopHash     = 5 // minimum tophash for a normal filled cell.

	// flags
	iterator     = 1 // there may be an iterator using buckets
	oldIterator  = 2 // there may be an iterator using oldbuckets
	hashWriting  = 4 // a goroutine is writing to the map
	sameSizeGrow = 8 // the current map growth is to a new map of the same size

	// sentinel bucket ID for iterator checks
	noCheck = 1<<(8*sys.PtrSize) - 1
)

// isEmpty reports whether the given tophash array entry represents an empty bucket entry.
func isEmpty(x uint8) bool {
	return x <= emptyOne
}

// A header for a Go map.
type hmap struct {
	// Note: the format of the hmap is also encoded in cmd/compile/internal/gc/reflect.go.
	// Make sure this stays in sync with the compiler's definition.
	count int // # live cells == size of map.  Must be first (used by len() builtin)
	// 状态标志（迭代、写入等）
	flags uint8
	// 桶数量的对数（实际桶数 = 2^B）
	B uint8 // log_2 of # of buckets (can hold up to loadFactor * 2^B items)
	// 溢出桶的近似数量
	noverflow uint16 // approximate number of overflow buckets; see incrnoverflow for details
	// 哈希种子
	hash0 uint32 // hash seed

	// 当前桶数组指针，桶中的元素是：type bmap struct
	buckets unsafe.Pointer // array of 2^B Buckets. may be nil if count==0.
	// 扩容时的旧桶数组指针
	oldbuckets unsafe.Pointer // previous bucket array of half the size, non-nil only when growing
	// 迁移进度计数器
	nevacuate uintptr // progress counter for evacuation (buckets less than this have been evacuated)
	// 可选字段（溢出桶、额外信息）
	extra *mapextra // optional fields
}

/*
	mapextra 是 Go 语言哈希表（hmap）的扩展元数据，用于处理某些特定场景下的内存管理和优化。

	并非所有 map 都会用到该结构体，仅在以下条件满足时才会分配：
		1. 键（key）和值（elem）均不包含指针（如 map[int]int）。
		2. 键和值均为内联类型（非接口或复杂类型，如 map[int]int）。

	为什么这么设计？
	1. 减少 GC 扫描开销：
		若键值无指针，桶内存无需 GC 扫描，大幅减少扫描范围。
		溢出桶指针通过 mapextra 集中管理，避免 GC 遗漏。

	2. 内存分配优化：
		预分配溢出桶通过 nextOverflow 缓存，减少运行时内存分配次数。
		溢出桶复用降低内存碎片。

	3. 内存布局紧凑：
		键值分离存储（先存所有键，再存所有值）避免因类型对齐导致的填充，节省内存。
		（例如 map[int64]int8 的键值对无额外填充）
*/
// mapextra holds fields that are not present on all maps.
type mapextra struct {
	// If both key and elem do not contain pointers and are inline, then we mark bucket
	// type as containing no pointers. This avoids scanning such maps.
	// However, bmap.overflow is a pointer. In order to keep overflow buckets
	// alive, we store pointers to all overflow buckets in hmap.extra.overflow and hmap.extra.oldoverflow.
	// overflow and oldoverflow are only used if key and elem do not contain pointers.
	// overflow contains overflow buckets for hmap.buckets.
	// oldoverflow contains overflow buckets for hmap.oldbuckets.
	// The indirection allows to store a pointer to the slice in hiter.
	// 当前桶数组的溢出桶列表
	overflow *[]*bmap
	// 扩容时旧桶数组的溢出桶列表
	oldoverflow *[]*bmap

	// nextOverflow holds a pointer to a free overflow bucket.
	// 预分配的空闲溢出桶链表头
	/*
		内存预分配：Go 在初始化 map 的桶数组时，会预分配一些溢出桶，形成单向链表。
		减少分配次数：当需要新溢出桶时，直接从 nextOverflow 链表中获取，避免频繁调用内存分配器。
	*/
	nextOverflow *bmap
}

/*
这只是表面(src/runtime/hashmap.go)的结构，编译期间会给它加料，动态地创建一个新的结构：
type bmap struct {
    tophash  [8]uint8

	// 这里key和value是分开存储到两个数组中的。
	// 这样的设计使得内存布局更加紧凑，减少了内存对齐和填充的开销。
	// 这在某些情况下（例如，存储不同类型的键和值: map[int64]int16）可以显著提高内存使用效率。
	// 性能: 通过将键和元素紧密存储，Go 的 map 实现可以提高缓存的局部性，从而提高访问速度。
	keys     [8]keytype
    values   [8]valuetype

	pad      uintptr
	// 处理哈希冲突：
	// 当一个桶中的键值对数量超过了桶的容量时，新的键值对将被存储在溢出桶中。
 	// 溢出桶是一个链表结构，指向其他 bmap 结构体，以便存储更多的键值对。
	// 使用溢出指针来处理哈希冲突，使得 Go 的 map 能够在高负载情况下仍然保持良好的性能。
    overflow uintptr
}
Note:
当 map 的 key 和 value 都不是指针，并且 size 都小于 128 字节的情况下，会把 bmap 标记为不含指针，这样可以避免 gc 时扫描整个 hmap。
但是，我们看 bmap 其实有一个 overflow 的字段，是指针类型的，破坏了 bmap 不含指针的设想，这时会把 overflow 移动到 extra 字段来。
*/
/*
	每个 bucket 设计成最多只能放 8 个 key-value 对，
	如果有第 9 个 key-value 落入当前的 bucket，那就需要再构建一个 bucket ，通过 overflow 指针连接起来。
*/
// A bucket for a Go map.
type bmap struct {
	/*
		当一个 槽 的 tophash 值小于 minTopHash 时，标志这个 槽 的迁移状态。
	*/
	// tophash generally contains the top byte of the hash value
	// for each key in this bucket. If tophash[0] < minTopHash,
	// tophash[0] is a bucket evacuation state instead.

	// tophash 数组存储每个键的哈希值的最高字节。这个字节通常用于快速比较哈希值，以确定键是否可能匹配。
	// 为什么这里说的是可能，因为即使哈希值对上了，还有比较key，才能完全确定。
	// bucketCnt == 8
	tophash [bucketCnt]uint8
	// Followed by bucketCnt keys and then bucketCnt elems.
	// NOTE: packing all the keys together and then all the elems together makes the
	// code a bit more complicated than alternating key/elem/key/elem/... but it allows
	// us to eliminate padding which would be needed for, e.g., map[int64]int8.
	// Followed by an overflow pointer.
}

// A hash iteration structure.
// If you modify hiter, also change cmd/compile/internal/gc/reflect.go to indicate
// the layout of this structure.
type hiter struct {
	key         unsafe.Pointer // Must be in first position.  Write nil to indicate iteration end (see cmd/internal/gc/range.go).
	elem        unsafe.Pointer // Must be in second position (see cmd/internal/gc/range.go).
	t           *maptype
	h           *hmap
	buckets     unsafe.Pointer // bucket ptr at hash_iter initialization time
	bptr        *bmap          // current bucket
	overflow    *[]*bmap       // keeps overflow buckets of hmap.buckets alive
	oldoverflow *[]*bmap       // keeps overflow buckets of hmap.oldbuckets alive
	startBucket uintptr        // bucket iteration started at
	offset      uint8          // intra-bucket offset to start from during iteration (should be big enough to hold bucketCnt-1)
	wrapped     bool           // already wrapped around from end of bucket array to beginning
	B           uint8
	i           uint8
	bucket      uintptr
	checkBucket uintptr
}

// bucketShift returns 1<<b, optimized for code generation.
// bucketShift 函数用于安全高效地计算 2 的 b 次方（即 1 << b），通过掩码操作确保位移量在系统位数范围内，避免溢出。
func bucketShift(b uint8) uintptr {
	// Masking the shift amount allows overflow checks to be elided.
	/*
		掩码值：sys.PtrSize*8 - 1
			32 位系统：4*8 -1 = 31 → 二进制 0b11111（5 位掩码）。
			64 位系统：8*8 -1 = 63 → 二进制 0b111111（6 位掩码）。
		作用：将 b 的大小限制在 [0, 31]（32 位）或 [0, 63]（64 位）范围内，避免位移量超出系统位数导致的未定义行为。

		设计意义
			(1) 安全性
				防止溢出：通过掩码确保位移量不超过系统位数，避免因 b 过大导致结果不可控。
				防御性编程：即使 b 错误传入超大值（如 b = 255），结果仍为有效位移量。
			(2) 性能优化
				省略溢出检查：掩码操作替代显式的条件判断（如 if b >= 64），减少分支预测开销。
				编译优化友好：位运算直接映射到硬件指令，提升代码生成效率。
			(3) 应用场景
				哈希表桶数计算：Go map 的桶数始终为 2 的幂次（2^B），通过 bucketShift(B) 快速计算桶数。
				内存对齐：确保桶数组大小适配系统位数，提升内存访问效率。

		uintptr(1) << (b & (sys.PtrSize*8 - 1)): 将1左移b位。
		假设b=5: 1左移5位=2^5=32
	*/
	return uintptr(1) << (b & (sys.PtrSize*8 - 1))
}

// bucketMask returns 1<<b - 1, optimized for code generation.
func bucketMask(b uint8) uintptr {
	// 假设b=5，则bucketShift(b)=32
	// 这里-1，是为了得到5位的掩码：2^5=32=100000，32-1=100000-1=11111 得到5位的掩码。
	// 掩码的作用是：为了去hash的低5位，用于定位桶数组的索引。即找到哪个桶。
	return bucketShift(b) - 1
}

// tophash calculates the tophash value for hash.
func tophash(hash uintptr) uint8 {
	// 提取哈希值高 8 位:
	// 		64 位系统：sys.PtrSize*8 = 64 → 右移 56 位 → 提取哈希值最高 8 位。
	// 		目的：用哈希值的最高 8 位作为快速筛选的标记（tophash），减少键比较次数。
	top := uint8(hash >> (sys.PtrSize*8 - 8))
	// 规避特殊标记:
	// 		原因：map 桶的 tophash 需要与以下特殊状态区分：0->4
	//		调整后：确保 tophash ∈ [5, 255]，避免与特殊标记冲突。
	if top < minTopHash {
		top += minTopHash
	}
	return top
}

func evacuated(b *bmap) bool {
	h := b.tophash[0]
	return h > emptyOne && h < minTopHash
}

func (b *bmap) overflow(t *maptype) *bmap {
	return *(**bmap)(add(unsafe.Pointer(b), uintptr(t.bucketsize)-sys.PtrSize))
}

func (b *bmap) setoverflow(t *maptype, ovf *bmap) {
	*(**bmap)(add(unsafe.Pointer(b), uintptr(t.bucketsize)-sys.PtrSize)) = ovf
}

func (b *bmap) keys() unsafe.Pointer {
	return add(unsafe.Pointer(b), dataOffset)
}

// incrnoverflow increments h.noverflow.
// noverflow counts the number of overflow buckets.
// This is used to trigger same-size map growth.
// See also tooManyOverflowBuckets.
// To keep hmap small, noverflow is a uint16.
// When there are few buckets, noverflow is an exact count.
// When there are many buckets, noverflow is an approximate count.
func (h *hmap) incrnoverflow() {
	// We trigger same-size map growth if there are
	// as many overflow buckets as buckets.
	// We need to be able to count to 1<<h.B.
	if h.B < 16 {
		h.noverflow++
		return
	}
	// Increment with probability 1/(1<<(h.B-15)).
	// When we reach 1<<15 - 1, we will have approximately
	// as many overflow buckets as buckets.
	mask := uint32(1)<<(h.B-15) - 1
	// Example: if h.B == 18, then mask == 7,
	// and fastrand & 7 == 0 with probability 1/8.
	if fastrand()&mask == 0 {
		h.noverflow++
	}
}

func (h *hmap) newoverflow(t *maptype, b *bmap) *bmap {
	var ovf *bmap
	if h.extra != nil && h.extra.nextOverflow != nil {
		// We have preallocated overflow buckets available.
		// See makeBucketArray for more details.
		ovf = h.extra.nextOverflow
		if ovf.overflow(t) == nil {
			// We're not at the end of the preallocated overflow buckets. Bump the pointer.
			h.extra.nextOverflow = (*bmap)(add(unsafe.Pointer(ovf), uintptr(t.bucketsize)))
		} else {
			// This is the last preallocated overflow bucket.
			// Reset the overflow pointer on this bucket,
			// which was set to a non-nil sentinel value.
			ovf.setoverflow(t, nil)
			h.extra.nextOverflow = nil
		}
	} else {
		ovf = (*bmap)(newobject(t.bucket))
	}
	h.incrnoverflow()
	if t.bucket.ptrdata == 0 {
		h.createOverflow()
		*h.extra.overflow = append(*h.extra.overflow, ovf)
	}
	b.setoverflow(t, ovf)
	return ovf
}

func (h *hmap) createOverflow() {
	if h.extra == nil {
		h.extra = new(mapextra)
	}
	if h.extra.overflow == nil {
		h.extra.overflow = new([]*bmap)
	}
}

func makemap64(t *maptype, hint int64, h *hmap) *hmap {
	if int64(int(hint)) != hint {
		hint = 0
	}
	return makemap(t, int(hint), h)
}

// makemap_small implements Go map creation for make(map[k]v) and
// make(map[k]v, hint) when hint is known to be at most bucketCnt
// at compile time and the map needs to be allocated on the heap.
func makemap_small() *hmap {
	h := new(hmap)
	h.hash0 = fastrand()
	return h
}

// makemap implements Go map creation for make(map[k]v, hint).
// If the compiler has determined that the map or the first bucket
// can be created on the stack, h and/or bucket may be non-nil.
// If h != nil, the map can be created directly in h.
// If h.buckets != nil, bucket pointed to can be used as the first bucket.
func makemap(t *maptype, hint int, h *hmap) *hmap {
	mem, overflow := math.MulUintptr(uintptr(hint), t.bucket.size)
	if overflow || mem > maxAlloc {
		hint = 0
	}

	// 0. new 一个 hmap
	// initialize Hmap
	if h == nil {
		h = new(hmap)
	}
	// 1. 通过 fastrand 创建一个随机的哈希种子
	h.hash0 = fastrand()

	// Find the size parameter B which will hold the requested # of elements.
	// For hint < 0 overLoadFactor returns false since hint < bucketCnt.
	B := uint8(0)
	// 2. 根据传入的 hint 计算出需要的最小需要的桶的数量
	for overLoadFactor(hint, B) {
		B++
	}
	h.B = B

	// allocate initial hash table
	// if B == 0, the buckets field is allocated lazily later (in mapassign)
	// If hint is large zeroing this memory could take a while.
	// 3. 使用 makeBucketArray创建用于保存桶的数组，
	// 这个方法其实就是根据传入的 B，计算出的需要创建的桶数量，在内存中分配一片连续的空间用于存储数据，
	// 在创建桶的过程中还会额外创建一些用于保存溢出数据的桶，数量是 2^(B-4) 个。
	if h.B != 0 {
		var nextOverflow *bmap
		h.buckets, nextOverflow = makeBucketArray(t, h.B, nil)
		if nextOverflow != nil {
			h.extra = new(mapextra)
			h.extra.nextOverflow = nextOverflow
		}
	}

	// 4. 初始化完成返回hmap指针。
	return h
}

// makeBucketArray initializes a backing array for map buckets.
// 1<<b is the minimum number of buckets to allocate.
// dirtyalloc should either be nil or a bucket array previously
// allocated by makeBucketArray with the same t and b parameters.
// If dirtyalloc is nil a new backing array will be alloced and
// otherwise dirtyalloc will be cleared and reused as backing array.
func makeBucketArray(t *maptype, b uint8, dirtyalloc unsafe.Pointer) (buckets unsafe.Pointer, nextOverflow *bmap) {
	base := bucketShift(b)
	nbuckets := base
	// For small b, overflow buckets are unlikely.
	// Avoid the overhead of the calculation.
	if b >= 4 {
		// Add on the estimated number of overflow buckets
		// required to insert the median number of elements
		// used with this value of b.
		nbuckets += bucketShift(b - 4)
		sz := t.bucket.size * nbuckets
		up := roundupsize(sz)
		if up != sz {
			nbuckets = up / t.bucket.size
		}
	}

	if dirtyalloc == nil {
		buckets = newarray(t.bucket, int(nbuckets))
	} else {
		// dirtyalloc was previously generated by
		// the above newarray(t.bucket, int(nbuckets))
		// but may not be empty.
		buckets = dirtyalloc
		size := t.bucket.size * nbuckets
		if t.bucket.ptrdata != 0 {
			memclrHasPointers(buckets, size)
		} else {
			memclrNoHeapPointers(buckets, size)
		}
	}

	if base != nbuckets {
		// We preallocated some overflow buckets.
		// To keep the overhead of tracking these overflow buckets to a minimum,
		// we use the convention that if a preallocated overflow bucket's overflow
		// pointer is nil, then there are more available by bumping the pointer.
		// We need a safe non-nil pointer for the last overflow bucket; just use buckets.
		nextOverflow = (*bmap)(add(buckets, base*uintptr(t.bucketsize)))
		last := (*bmap)(add(buckets, (nbuckets-1)*uintptr(t.bucketsize)))
		last.setoverflow(t, (*bmap)(buckets))
	}
	return buckets, nextOverflow
}

/*
	看个实例：
	func main() {
		m := map[int64]string
		m[1] = "a"

		// 使用m[1]访问其中的一个key，是如何调用到相应的源码的？
		fmt.Println(m[1])
	}

	在 Go 语言中，当你通过 m[1] 访问一个 map[int64]string 类型的 map 时，底层会通过编译器和运行时协作，最终调用到 mapaccess1 等运行时函数。
	也就是说，是在编译阶段，编译器将 m[1] 转为对 mapaccess1 函数的调用：
	// 伪代码：实际由编译器生成
	valPtr := mapaccess1(maptype_of_m, &m, unsafe.Pointer(&key))
	val := *(*string)(valPtr) // 将指针解引用为 string 类型
*/

/*
	这个函数是用来从map中获取某个键对应的值的指针，即使键不存在，也会返回零值的指针。（如 int 返回 0 的地址）
	mapaccess1 通过多级哈希定位、高效内存布局和并发安全机制，实现了 Go map 的高性能查找。
	其设计充分权衡了速度、内存与并发安全性，是 Go 运行时高效数据结构的典型代表。

	关键设计点
		哈希分片与掩码：通过哈希低位确定 Bucket，保证数据均匀分布。
		TopHash 快速过滤：减少全键比较次数，提升查找效率。
		扩容透明处理：在扩容期间同时查找新旧 Bucket，确保数据迁移不影响读操作。
		内存布局优化：紧凑存储键值对，利用 CPU 缓存行提升访问速度。
		并发安全机制：通过 flags 标志位检测并发写操作，快速失败避免数据竞争。
		零值返回策略：避免返回 nil，简化调用方错误处理，但需注意潜在 GC 影响。

	性能优化
		位运算替代除法：Bucket 索引计算使用位掩码而非取模运算。
		提前终止搜索：emptyRest 标记减少不必要的槽位检查。
		间接存储节省空间：对大对象存储指针而非值，减少 Bucket 内存占用。

*/
// mapaccess1 returns a pointer to h[key].  Never returns nil, instead
// it will return a reference to the zero object for the elem type if
// the key is not in the map.
// NOTE: The returned pointer may keep the whole map live, so don't
// hold onto it for very long.
func mapaccess1(t *maptype, h *hmap, key unsafe.Pointer) unsafe.Pointer {
	// 竞态检测（Race Detector）：
	// 若启用竞态检测（-race），记录当前 map 的读操作，用于检测并发读写冲突。
	if raceenabled && h != nil {
		callerpc := getcallerpc()
		pc := funcPC(mapaccess1)
		racereadpc(unsafe.Pointer(h), callerpc, pc)
		raceReadObjectPC(t.key, key, callerpc, pc)
	}
	// 内存检查（Memory Sanitizer）：
	// 若启用内存检查（-msan），验证 key 的内存区域是否合法。
	if msanenabled && h != nil {
		msanread(key, t.key.size)
	}
	// 空 map 直接返回零值:
	// 即即使一个map没有初始化，通过 map[key] 访问也不会panic，只会返回零值。
	if h == nil || h.count == 0 {
		// 哈希函数可能 panic：某些键类型（如函数、接口）的哈希函数可能 panic，需在此处提前触发。
		if t.hashMightPanic() {
			t.hasher(key, 0) // see issue 23734
		}
		return unsafe.Pointer(&zeroVal[0])
	}
	// 检测并发写操作：
	// 若发现其他协程正在写入 map（h.flags 包含 hashWriting 标志），直接抛出异常。
	// Go map 非线程安全：这是 Go 防止并发读写导致内存损坏的关键机制。
	if h.flags&hashWriting != 0 {
		throw("concurrent map read and map write")
	}
	// 哈希函数调用：
	// 使用类型特定的哈希函数 t.hasher，结合 map 的随机种子 h.hash0，计算键的哈希值。
	// 哈希随机化：h.hash0 在 map 创建时生成，防止哈希碰撞攻击。
	hash := t.hasher(key, uintptr(h.hash0))
	// 比如 B=5，那 m 就是31。桶的数量 = m+1 = 32个桶
	m := bucketMask(h.B)
	// 1. (hash&m):	这里的目的是：将hash和桶数据做取余运算：hash%桶数量，来找到hash对应的桶。
	//    但是取余运算性能太低，所以改成位运算。
	//    位运算取余的原理：当模数 m 是 2 的幂次方（如 32 = 2^5）时，取余运算 n % m 等价于保留 n 的二进制低 log2(m) 【log2(32) = 5】 位。
	//	  举个例子：n % 32 == n & (32 - 1)  // 即 n & 31
	// 2. uintptr(t.bucketsize): 这里只是单纯将 t.bucketsize 属性的值转为 unitptr类型，值本身是没有变的
	// 3. (add(h.buckets, (hash&m)*uintptr(t.bucketsize))): h.buckets 是桶数组的起始地址，通过ji's偏移量 (hash&m)*uintptr(t.bucketsize) 找到目标桶 b。
	// 4. 所以，b是目标桶，或者说是目标桶的地址。
	b := (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
	// oldbuckets 不为 nil，说明发生了扩容
	if c := h.oldbuckets; c != nil {
		// 如果不是等量扩容，旧桶数量是当前的一半
		/*
			扩容行为
				普通扩容（非同尺寸）：
					创建两倍于当前 bucket 数量的新 bucket 数组（h.B++）。
					数据迁移到新 buckets，分散键值对以减少冲突。
				同尺寸扩容：
					保持 bucket 数量不变（h.B 不变）。
					重新整理现有数据，减少溢出链长度，提升访问效率。
		*/
		if !h.sameSizeGrow() {
			// There used to be half as many buckets; mask down one more power of two.
			m >>= 1
		}
		// 重新计算：求出 key 在老的 map 中的 bucket 位置
		oldb := (*bmap)(add(c, (hash&m)*uintptr(t.bucketsize)))
		//若旧桶未迁移，继续在旧桶中查找 那就在老的 bucket 中寻找
		if !evacuated(oldb) {
			b = oldb
		}
	}
	// 计算出高 8 位的 hash：即右移 56 位，剩下原来的高8位
	// tophash 用于快速跳过不匹配的键，减少全键比较次数。
	top := tophash(hash)
bucketloop:
	// 开始寻找key
	// 刚开始的时候 bmap肯定不为nil，遍历完当前bmap的8个位置，开始遍历 overflow
	for ; b != nil; b = b.overflow(t) {
		// 内层循环：遍历 Bucket 内 8 个槽位
		for i := uintptr(0); i < bucketCnt; i++ {
			if b.tophash[i] != top {
				// 后续槽位均为空，提前终止
				if b.tophash[i] == emptyRest {
					break bucketloop
				}
				continue
			}
			// tophash 匹配，定位到 key 的位置
			// b 是目标桶的地址，通过位移计算，得出目标key的地址：k
			k := add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
			// key 是指针
			if t.indirectkey() {
				// 解引用，即取key的值
				k = *((*unsafe.Pointer)(k))
			}
			// 如果 key 相等。使用hash的前8位表示一个kv，是有冲突的可能的，所以找到tophash之后，还需要比较key是否相同
			if t.key.equal(key, k) {
				// 定位到 value 的位置
				e := add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
				// value 解引用
				if t.indirectelem() {
					e = *((*unsafe.Pointer)(e))
				}
				return e
			}
		}
	}
	// 键不存在处理：遍历所有可能位置未找到匹配键时，返回全局零值指针。
	return unsafe.Pointer(&zeroVal[0])
}

func mapaccess2(t *maptype, h *hmap, key unsafe.Pointer) (unsafe.Pointer, bool) {
	if raceenabled && h != nil {
		callerpc := getcallerpc()
		pc := funcPC(mapaccess2)
		racereadpc(unsafe.Pointer(h), callerpc, pc)
		raceReadObjectPC(t.key, key, callerpc, pc)
	}
	if msanenabled && h != nil {
		msanread(key, t.key.size)
	}
	if h == nil || h.count == 0 {
		if t.hashMightPanic() {
			t.hasher(key, 0) // see issue 23734
		}
		return unsafe.Pointer(&zeroVal[0]), false
	}
	if h.flags&hashWriting != 0 {
		throw("concurrent map read and map write")
	}
	hash := t.hasher(key, uintptr(h.hash0))
	m := bucketMask(h.B)
	b := (*bmap)(unsafe.Pointer(uintptr(h.buckets) + (hash&m)*uintptr(t.bucketsize)))
	if c := h.oldbuckets; c != nil {
		if !h.sameSizeGrow() {
			// There used to be half as many buckets; mask down one more power of two.
			m >>= 1
		}
		oldb := (*bmap)(unsafe.Pointer(uintptr(c) + (hash&m)*uintptr(t.bucketsize)))
		if !evacuated(oldb) {
			b = oldb
		}
	}
	top := tophash(hash)
bucketloop:
	for ; b != nil; b = b.overflow(t) {
		for i := uintptr(0); i < bucketCnt; i++ {
			if b.tophash[i] != top {
				if b.tophash[i] == emptyRest {
					break bucketloop
				}
				continue
			}
			k := add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
			if t.indirectkey() {
				k = *((*unsafe.Pointer)(k))
			}
			if t.key.equal(key, k) {
				e := add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
				if t.indirectelem() {
					e = *((*unsafe.Pointer)(e))
				}
				return e, true
			}
		}
	}
	return unsafe.Pointer(&zeroVal[0]), false
}

// returns both key and elem. Used by map iterator
func mapaccessK(t *maptype, h *hmap, key unsafe.Pointer) (unsafe.Pointer, unsafe.Pointer) {
	if h == nil || h.count == 0 {
		return nil, nil
	}
	hash := t.hasher(key, uintptr(h.hash0))
	m := bucketMask(h.B)
	b := (*bmap)(unsafe.Pointer(uintptr(h.buckets) + (hash&m)*uintptr(t.bucketsize)))
	if c := h.oldbuckets; c != nil {
		if !h.sameSizeGrow() {
			// There used to be half as many buckets; mask down one more power of two.
			m >>= 1
		}
		oldb := (*bmap)(unsafe.Pointer(uintptr(c) + (hash&m)*uintptr(t.bucketsize)))
		if !evacuated(oldb) {
			b = oldb
		}
	}
	top := tophash(hash)
bucketloop:
	for ; b != nil; b = b.overflow(t) {
		for i := uintptr(0); i < bucketCnt; i++ {
			if b.tophash[i] != top {
				if b.tophash[i] == emptyRest {
					break bucketloop
				}
				continue
			}
			k := add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
			if t.indirectkey() {
				k = *((*unsafe.Pointer)(k))
			}
			if t.key.equal(key, k) {
				e := add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
				if t.indirectelem() {
					e = *((*unsafe.Pointer)(e))
				}
				return k, e
			}
		}
	}
	return nil, nil
}

func mapaccess1_fat(t *maptype, h *hmap, key, zero unsafe.Pointer) unsafe.Pointer {
	e := mapaccess1(t, h, key)
	if e == unsafe.Pointer(&zeroVal[0]) {
		return zero
	}
	return e
}

func mapaccess2_fat(t *maptype, h *hmap, key, zero unsafe.Pointer) (unsafe.Pointer, bool) {
	e := mapaccess1(t, h, key)
	if e == unsafe.Pointer(&zeroVal[0]) {
		return zero, false
	}
	return e, true
}

/*
	关键设计思想
		延迟初始化：若 h.buckets 为空，首次插入数据时，才分配初始 Bucket 数组（默认大小为 1 << B）。
		惰性扩容：仅在插入时检查负载因子，逐步迁移数据，平衡性能与内存。
		写时复制：通过 oldbuckets 保留旧数据，保证扩容期间读操作不受影响。
		内存局部性：Bucket 内键值紧凑存储，利用 CPU 缓存行提升访问速度。
		间接存储优化：对大对象存储指针，减少 Bucket 内存占用及复制开销。
		并发安全：通过 flags 标志位实现轻量级写锁，避免全局锁竞争。

	性能影响
		哈希冲突：链表法处理冲突，最坏情况下退化为 O(n)，但良好的哈希分布可保持 O(1)。
		内存重用：溢出桶通过预分配和复用减少 GC 压力。
		扩容代价：分摊到每次写入操作，避免一次性大延迟。
*/
// Like mapaccess, but allocates a slot for the key if it is not present in the map.
// map赋值，第一阶段：校验和初始化
func mapassign(t *maptype, h *hmap, key unsafe.Pointer) unsafe.Pointer {
	// 判断 hmap 是否已经初始化（是否为 nil）
	if h == nil {
		// 未初始化的map，通过m[key]获取不会报错，会返回零值
		// 未初始化的map，通过m[key]=val赋值，会panic
		panic(plainError("assignment to entry in nil map"))
	}
	if raceenabled {
		callerpc := getcallerpc()
		pc := funcPC(mapassign)
		racewritepc(unsafe.Pointer(h), callerpc, pc)
		raceReadObjectPC(t.key, key, callerpc, pc)
	}
	if msanenabled {
		msanread(key, t.key.size)
	}
	// 判断是否并发读写 map，若是则抛出异常
	if h.flags&hashWriting != 0 {
		throw("concurrent map writes")
	}
	// 根据 key 的不同类型调用不同的 hash 方法计算得出 hash 值
	hash := t.hasher(key, uintptr(h.hash0))

	// Set hashWriting after calling t.hasher, since t.hasher may panic,
	// in which case we have not actually done a write.
	// 设置 flags 标志位，表示有一个 goroutine 正在写入数据。因为 alg.hash 有可能出现 panic 导致异常
	h.flags ^= hashWriting

	// 延迟初始化:
	// 判断 buckets 是否为 nil，若是则调用 newobject 根据当前 bucket 大小进行分配
	// 初始化时没有初始 buckets，那么它在第一次赋值时就会对 buckets 分配
	if h.buckets == nil {
		h.buckets = newobject(t.bucket) // newarray(t.bucket, 1)
	}

again:
	/*
		渐进式扩容：在写入时触发部分数据迁移（每次迁移 1-2 个 Bucket），避免一次性迁移的延迟峰值。
		重试机制：迁移后 Bucket 布局变化，需重新计算索引。
	*/
	// 根据低八位计算得到 bucket 的内存地址
	bucket := hash & bucketMask(h.B)
	// 判断是否正在扩容，若正在扩容中则先迁移再接着处理
	if h.growing() {
		growWork(t, h, bucket)
	}
	// 计算并得到 bucket 的 bmap 指针地址
	b := (*bmap)(unsafe.Pointer(uintptr(h.buckets) + bucket*uintptr(t.bucketsize)))
	// 计算 key hash 高八位用于查找 Key
	top := tophash(hash)

	var inserti *uint8         // 存高8位哈希值
	var insertk unsafe.Pointer // 存 key 的内存地址
	var elem unsafe.Pointer    // 存 value 的内存地址
bucketloop:
	for {
		// 迭代 bmap 中的每一个 位置（共 8 个）
		for i := uintptr(0); i < bucketCnt; i++ {
			// 对比 bucket.tophash 与 top（高八位）是否一致
			if b.tophash[i] != top {
				// 若不一致，判断是否为空槽
				// 有两种情况，第一种是没有插入过。第二种是插入后被删除
				if isEmpty(b.tophash[i]) && inserti == nil {
					// 把该位置标识为可插入 tophash 位置,这里就是第一个可以插入数据的地方
					inserti = &b.tophash[i]
					insertk = add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
					elem = add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
				}
				if b.tophash[i] == emptyRest {
					break bucketloop
				}
				continue
			}
			// 若是匹配（也就是原本已经存在），则进行更新。最后跳出并返回 value 的内存地址
			k := add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
			if t.indirectkey() {
				k = *((*unsafe.Pointer)(k))
			}
			// 键相等性判断：使用类型特定的 equal 函数进行深度比较。
			if !t.key.equal(key, k) {
				continue
			}
			// already have a mapping for key. Update it.
			if t.needkeyupdate() {
				typedmemmove(t.key, k, key)
			}
			elem = add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
			goto done
		}
		// 迭代完 普通的 bmap，如果还没有合适的位置插入，继续迭代 overflow的 bmap，直到找到合适的插入位置。
		ovf := b.overflow(t)
		if ovf == nil {
			break
		}
		b = ovf
	}

	// Did not find mapping for key. Allocate new cell & add entry.

	// If we hit the max load factor or we have too many overflow buckets,
	// and we're not already in the middle of growing, start growing.
	// 若同时满足三个条件：
	// 1. 没有正在进行扩容。
	// 2. 触发最大 LoadFactor 、
	// 3. 存在过多溢出桶 overflow buckets、【溢出桶的数量大于正常桶的数量或溢出桶的数量大于 2^15】
	// 就会进行扩容
	if !h.growing() && (overLoadFactor(h.count+1, h.B) || tooManyOverflowBuckets(h.noverflow, h.B)) {
		hashGrow(t, h)
		goto again // Growing the table invalidates everything, so try again
	}

	// 经过前面迭代寻找动作，若没有找到可插入的位置，意味着当前的所有桶都满了， 将重新分配一个新溢出桶用于插入动作。
	// 最后再在上一步申请的新插入位置，存储键值对，返回该值的内存地址
	if inserti == nil {
		// all current buckets are full, allocate a new one.
		newb := h.newoverflow(t, b)
		inserti = &newb.tophash[0]
		insertk = add(unsafe.Pointer(newb), dataOffset)
		elem = add(insertk, bucketCnt*uintptr(t.keysize))
	}

	// store new key/elem at insert position
	// 间接存储处理：键或值为指针类型时，额外分配堆内存并存储指针。
	// 存储 插入的kv以及hash值
	if t.indirectkey() {
		kmem := newobject(t.key)
		*(*unsafe.Pointer)(insertk) = kmem
		insertk = kmem
	}
	if t.indirectelem() {
		vmem := newobject(t.elem)
		*(*unsafe.Pointer)(elem) = vmem
	}
	typedmemmove(t.key, insertk, key)
	*inserti = top
	h.count++

done:
	if h.flags&hashWriting == 0 {
		throw("concurrent map writes")
	}
	// 修正 正在写标志
	h.flags &^= hashWriting
	if t.indirectelem() {
		elem = *((*unsafe.Pointer)(elem))
	}
	return elem
}

func mapdelete(t *maptype, h *hmap, key unsafe.Pointer) {
	if raceenabled && h != nil {
		callerpc := getcallerpc()
		pc := funcPC(mapdelete)
		racewritepc(unsafe.Pointer(h), callerpc, pc)
		raceReadObjectPC(t.key, key, callerpc, pc)
	}
	if msanenabled && h != nil {
		msanread(key, t.key.size)
	}
	if h == nil || h.count == 0 {
		if t.hashMightPanic() {
			t.hasher(key, 0) // see issue 23734
		}
		return
	}
	if h.flags&hashWriting != 0 {
		throw("concurrent map writes")
	}

	hash := t.hasher(key, uintptr(h.hash0))

	// Set hashWriting after calling t.hasher, since t.hasher may panic,
	// in which case we have not actually done a write (delete).
	h.flags ^= hashWriting

	bucket := hash & bucketMask(h.B)
	if h.growing() {
		growWork(t, h, bucket)
	}
	b := (*bmap)(add(h.buckets, bucket*uintptr(t.bucketsize)))
	bOrig := b
	top := tophash(hash)
search:
	for ; b != nil; b = b.overflow(t) {
		for i := uintptr(0); i < bucketCnt; i++ {
			if b.tophash[i] != top {
				if b.tophash[i] == emptyRest {
					break search
				}
				continue
			}
			k := add(unsafe.Pointer(b), dataOffset+i*uintptr(t.keysize))
			k2 := k
			if t.indirectkey() {
				k2 = *((*unsafe.Pointer)(k2))
			}
			if !t.key.equal(key, k2) {
				continue
			}
			// Only clear key if there are pointers in it.
			if t.indirectkey() {
				*(*unsafe.Pointer)(k) = nil
			} else if t.key.ptrdata != 0 {
				memclrHasPointers(k, t.key.size)
			}
			e := add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+i*uintptr(t.elemsize))
			if t.indirectelem() {
				*(*unsafe.Pointer)(e) = nil
			} else if t.elem.ptrdata != 0 {
				memclrHasPointers(e, t.elem.size)
			} else {
				memclrNoHeapPointers(e, t.elem.size)
			}
			b.tophash[i] = emptyOne
			// If the bucket now ends in a bunch of emptyOne states,
			// change those to emptyRest states.
			// It would be nice to make this a separate function, but
			// for loops are not currently inlineable.
			if i == bucketCnt-1 {
				if b.overflow(t) != nil && b.overflow(t).tophash[0] != emptyRest {
					goto notLast
				}
			} else {
				if b.tophash[i+1] != emptyRest {
					goto notLast
				}
			}
			for {
				b.tophash[i] = emptyRest
				if i == 0 {
					if b == bOrig {
						break // beginning of initial bucket, we're done.
					}
					// Find previous bucket, continue at its last entry.
					c := b
					for b = bOrig; b.overflow(t) != c; b = b.overflow(t) {
					}
					i = bucketCnt - 1
				} else {
					i--
				}
				if b.tophash[i] != emptyOne {
					break
				}
			}
		notLast:
			h.count--
			break search
		}
	}

	if h.flags&hashWriting == 0 {
		throw("concurrent map writes")
	}
	h.flags &^= hashWriting
}

// mapiterinit initializes the hiter struct used for ranging over maps.
// The hiter struct pointed to by 'it' is allocated on the stack
// by the compilers order pass or on the heap by reflect_mapiterinit.
// Both need to have zeroed hiter since the struct contains pointers.
func mapiterinit(t *maptype, h *hmap, it *hiter) {
	if raceenabled && h != nil {
		callerpc := getcallerpc()
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapiterinit))
	}

	if h == nil || h.count == 0 {
		return
	}

	if unsafe.Sizeof(hiter{})/sys.PtrSize != 12 {
		throw("hash_iter size incorrect") // see cmd/compile/internal/gc/reflect.go
	}
	it.t = t
	it.h = h

	// grab snapshot of bucket state
	it.B = h.B
	it.buckets = h.buckets
	if t.bucket.ptrdata == 0 {
		// Allocate the current slice and remember pointers to both current and old.
		// This preserves all relevant overflow buckets alive even if
		// the table grows and/or overflow buckets are added to the table
		// while we are iterating.
		h.createOverflow()
		it.overflow = h.extra.overflow
		it.oldoverflow = h.extra.oldoverflow
	}

	// decide where to start
	r := uintptr(fastrand())
	if h.B > 31-bucketCntBits {
		r += uintptr(fastrand()) << 31
	}
	it.startBucket = r & bucketMask(h.B)
	it.offset = uint8(r >> h.B & (bucketCnt - 1))

	// iterator state
	it.bucket = it.startBucket

	// Remember we have an iterator.
	// Can run concurrently with another mapiterinit().
	if old := h.flags; old&(iterator|oldIterator) != iterator|oldIterator {
		atomic.Or8(&h.flags, iterator|oldIterator)
	}

	mapiternext(it)
}

func mapiternext(it *hiter) {
	h := it.h
	if raceenabled {
		callerpc := getcallerpc()
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapiternext))
	}
	if h.flags&hashWriting != 0 {
		throw("concurrent map iteration and map write")
	}
	t := it.t
	bucket := it.bucket
	b := it.bptr
	i := it.i
	checkBucket := it.checkBucket

next:
	if b == nil {
		if bucket == it.startBucket && it.wrapped {
			// end of iteration
			it.key = nil
			it.elem = nil
			return
		}
		if h.growing() && it.B == h.B {
			// Iterator was started in the middle of a grow, and the grow isn't done yet.
			// If the bucket we're looking at hasn't been filled in yet (i.e. the old
			// bucket hasn't been evacuated) then we need to iterate through the old
			// bucket and only return the ones that will be migrated to this bucket.
			oldbucket := bucket & it.h.oldbucketmask()
			b = (*bmap)(add(h.oldbuckets, oldbucket*uintptr(t.bucketsize)))
			if !evacuated(b) {
				checkBucket = bucket
			} else {
				b = (*bmap)(add(it.buckets, bucket*uintptr(t.bucketsize)))
				checkBucket = noCheck
			}
		} else {
			b = (*bmap)(add(it.buckets, bucket*uintptr(t.bucketsize)))
			checkBucket = noCheck
		}
		bucket++
		if bucket == bucketShift(it.B) {
			bucket = 0
			it.wrapped = true
		}
		i = 0
	}
	for ; i < bucketCnt; i++ {
		offi := (i + it.offset) & (bucketCnt - 1)
		if isEmpty(b.tophash[offi]) || b.tophash[offi] == evacuatedEmpty {
			// TODO: emptyRest is hard to use here, as we start iterating
			// in the middle of a bucket. It's feasible, just tricky.
			continue
		}
		k := add(unsafe.Pointer(b), dataOffset+uintptr(offi)*uintptr(t.keysize))
		if t.indirectkey() {
			k = *((*unsafe.Pointer)(k))
		}
		e := add(unsafe.Pointer(b), dataOffset+bucketCnt*uintptr(t.keysize)+uintptr(offi)*uintptr(t.elemsize))
		if checkBucket != noCheck && !h.sameSizeGrow() {
			// Special case: iterator was started during a grow to a larger size
			// and the grow is not done yet. We're working on a bucket whose
			// oldbucket has not been evacuated yet. Or at least, it wasn't
			// evacuated when we started the bucket. So we're iterating
			// through the oldbucket, skipping any keys that will go
			// to the other new bucket (each oldbucket expands to two
			// buckets during a grow).
			if t.reflexivekey() || t.key.equal(k, k) {
				// If the item in the oldbucket is not destined for
				// the current new bucket in the iteration, skip it.
				hash := t.hasher(k, uintptr(h.hash0))
				if hash&bucketMask(it.B) != checkBucket {
					continue
				}
			} else {
				// Hash isn't repeatable if k != k (NaNs).  We need a
				// repeatable and randomish choice of which direction
				// to send NaNs during evacuation. We'll use the low
				// bit of tophash to decide which way NaNs go.
				// NOTE: this case is why we need two evacuate tophash
				// values, evacuatedX and evacuatedY, that differ in
				// their low bit.
				if checkBucket>>(it.B-1) != uintptr(b.tophash[offi]&1) {
					continue
				}
			}
		}
		if (b.tophash[offi] != evacuatedX && b.tophash[offi] != evacuatedY) ||
			!(t.reflexivekey() || t.key.equal(k, k)) {
			// This is the golden data, we can return it.
			// OR
			// key!=key, so the entry can't be deleted or updated, so we can just return it.
			// That's lucky for us because when key!=key we can't look it up successfully.
			it.key = k
			if t.indirectelem() {
				e = *((*unsafe.Pointer)(e))
			}
			it.elem = e
		} else {
			// The hash table has grown since the iterator was started.
			// The golden data for this key is now somewhere else.
			// Check the current hash table for the data.
			// This code handles the case where the key
			// has been deleted, updated, or deleted and reinserted.
			// NOTE: we need to regrab the key as it has potentially been
			// updated to an equal() but not identical key (e.g. +0.0 vs -0.0).
			rk, re := mapaccessK(t, h, k)
			if rk == nil {
				continue // key has been deleted
			}
			it.key = rk
			it.elem = re
		}
		it.bucket = bucket
		if it.bptr != b { // avoid unnecessary write barrier; see issue 14921
			it.bptr = b
		}
		it.i = i + 1
		it.checkBucket = checkBucket
		return
	}
	b = b.overflow(t)
	i = 0
	goto next
}

// mapclear deletes all keys from a map.
func mapclear(t *maptype, h *hmap) {
	if raceenabled && h != nil {
		callerpc := getcallerpc()
		pc := funcPC(mapclear)
		racewritepc(unsafe.Pointer(h), callerpc, pc)
	}

	if h == nil || h.count == 0 {
		return
	}

	if h.flags&hashWriting != 0 {
		throw("concurrent map writes")
	}

	h.flags ^= hashWriting

	h.flags &^= sameSizeGrow
	h.oldbuckets = nil
	h.nevacuate = 0
	h.noverflow = 0
	h.count = 0

	// Keep the mapextra allocation but clear any extra information.
	if h.extra != nil {
		*h.extra = mapextra{}
	}

	// makeBucketArray clears the memory pointed to by h.buckets
	// and recovers any overflow buckets by generating them
	// as if h.buckets was newly alloced.
	_, nextOverflow := makeBucketArray(t, h.B, h.buckets)
	if nextOverflow != nil {
		// If overflow buckets are created then h.extra
		// will have been allocated during initial bucket creation.
		h.extra.nextOverflow = nextOverflow
	}

	if h.flags&hashWriting == 0 {
		throw("concurrent map writes")
	}
	h.flags &^= hashWriting
}

/*
	1. 触发翻倍扩容（2 倍扩容）
		当 元素数量超过负载因子阈值 时触发，目的是减少哈希冲突，提升查询效率。

	2. 触发等量扩容（整理扩容）
		当 overflow bucket 过多，但元素数量未超负载因子 时触发，目的是整理内存碎片，优化空间利用率。
*/
func hashGrow(t *maptype, h *hmap) {
	// If we've hit the load factor, get bigger.
	// Otherwise, there are too many overflow buckets,
	// so keep the same number of buckets and "grow" laterally.
	bigger := uint8(1)
	// 对应条件 2
	if !overLoadFactor(h.count+1, h.B) {
		// 进行等量的内存扩容，所以 B 不变
		bigger = 0
		h.flags |= sameSizeGrow
	}
	// 将老 buckets 挂到 buckets 上
	oldbuckets := h.buckets
	// 申请新的 buckets 空间
	newbuckets, nextOverflow := makeBucketArray(t, h.B+bigger, nil)

	//先把 h.flags 中 iterator 和 oldIterator 对应位清 0
	//如果 iterator 位为 1，把它转接到 oldIterator 位，使得 oldIterator 标志位变成1
	//可以理解为buckets 现在挂到了 oldBuckets 名下了，将对应的标志位也转接过去
	flags := h.flags &^ (iterator | oldIterator)
	if h.flags&iterator != 0 {
		flags |= oldIterator
	}
	// commit the grow (atomic wrt gc)
	// bigger := uint8(1)
	// B+1 相当于是原来 2 倍的空间
	h.B += bigger
	h.flags = flags
	h.oldbuckets = oldbuckets
	h.buckets = newbuckets
	// 搬迁进度为 0
	h.nevacuate = 0
	// overflow buckets 数为 0
	h.noverflow = 0

	if h.extra != nil && h.extra.overflow != nil {
		// Promote current overflow buckets to the old generation.
		if h.extra.oldoverflow != nil {
			throw("oldoverflow is not nil")
		}
		h.extra.oldoverflow = h.extra.overflow
		h.extra.overflow = nil
	}
	if nextOverflow != nil {
		if h.extra == nil {
			h.extra = new(mapextra)
		}
		h.extra.nextOverflow = nextOverflow
	}

	// the actual copying of the hash table data is done incrementally
	// by growWork() and evacuate().
}

/*
	loadFactor := count / (2^B)
	count 就是 map 的元素个数，2^B 表示 bucket 数量。

	我们知道，每个 bucket 有 8 个空位，在没有溢出，且所有的桶都装满了的情况下，装载因子算出来的结果是 8。
	因此当装载因子超过 6.5 时，表明很多 bucket 都快要装满了，查找效率和插入效率都变低了。在这个时候进行扩容是有必要的。
*/
// overLoadFactor reports whether count items placed in 1<<B buckets is over loadFactor.
func overLoadFactor(count int, B uint8) bool {
	// count：当前 map 中的键值对数量。
	// B：map 的 bucket 数组长度的对数，桶数量=2^B=(bucketShift(B)。
	// bucketCnt：每个 bucket 可容纳的键值对数量（固定为 8）。
	// loadFactorNum：负载因子分子（值为 13）。
	// loadFactorDen：负载因子分母（值为 2）。
	// 负载因子阈值：13/2 = 6.5。
	return count > bucketCnt && uintptr(count) > loadFactorNum*(bucketShift(B)/loadFactorDen)
	/*
		uintptr(count) > loadFactorNum*(bucketShift(B)/loadFactorDen)
		== uintptr(count) > (bucketShift(B)*loadFactorNum/loadFactorDen)
		== uintptr(count)/(bucketShift(B) > loadFactorNum/loadFactorDen)
		== map中键值对数量 / 桶数量 > 负载因子阈值：6.5

		估计是因为乘法运算比除法运算的效率高，所以才写成这样
	*/
}

// tooManyOverflowBuckets reports whether noverflow buckets is too many for a map with 1<<B buckets.
// Note that most of these overflow buckets must be in sparse use;
// if use was dense, then we'd have already triggered regular map growth.
func tooManyOverflowBuckets(noverflow uint16, B uint8) bool {
	// If the threshold is too low, we do extraneous work.
	// If the threshold is too high, maps that grow and shrink can hold on to lots of unused memory.
	// "too many" means (approximately) as many overflow buckets as regular buckets.
	// See incrnoverflow for more details.
	if B > 15 {
		B = 15
	}
	// The compiler doesn't see here that B < 16; mask B to generate shorter shift code.
	return noverflow >= uint16(1)<<(B&15)
}

// growing reports whether h is growing. The growth may be to the same size or bigger.
func (h *hmap) growing() bool {
	return h.oldbuckets != nil
}

// sameSizeGrow reports whether the current growth is to a map of the same size.
func (h *hmap) sameSizeGrow() bool {
	return h.flags&sameSizeGrow != 0
}

// noldbuckets calculates the number of buckets prior to the current map growth.
func (h *hmap) noldbuckets() uintptr {
	oldB := h.B
	if !h.sameSizeGrow() {
		oldB--
	}
	return bucketShift(oldB)
}

// oldbucketmask provides a mask that can be applied to calculate n % noldbuckets().
func (h *hmap) oldbucketmask() uintptr {
	return h.noldbuckets() - 1
}

func growWork(t *maptype, h *hmap, bucket uintptr) {
	// make sure we evacuate the oldbucket corresponding
	// to the bucket we're about to use
	// 搬迁正在使用的旧 bucket
	evacuate(t, h, bucket&h.oldbucketmask())

	// evacuate one more oldbucket to make progress on growing
	// 再搬迁一个 bucket，以加快搬迁进程
	if h.growing() {
		evacuate(t, h, h.nevacuate)
	}
}

func bucketEvacuated(t *maptype, h *hmap, bucket uintptr) bool {
	b := (*bmap)(add(h.oldbuckets, bucket*uintptr(t.bucketsize)))
	return evacuated(b)
}

// evacDst is an evacuation destination.
type evacDst struct {
	// 表示bucket 移动的目标地址
	b *bmap // current destination bucket
	// 指向 x,y 中 key/val 的 index
	i int // key/elem index into b
	// 指向 x，y 中的 key
	k unsafe.Pointer // pointer to current key storage
	// 指向 x，y 中的 value
	e unsafe.Pointer // pointer to current elem storage
}

func evacuate(t *maptype, h *hmap, oldbucket uintptr) {
	// 定位老的 bucket 地址
	b := (*bmap)(add(h.oldbuckets, oldbucket*uintptr(t.bucketsize)))
	// 计算容量 结果是 2^B，如 B = 5，结果为32
	newbit := h.noldbuckets()
	// 如果 b 没有被搬迁过
	if !evacuated(b) {
		// TODO: reuse overflow buckets instead of using new ones, if there
		// is no iterator using the old buckets.  (If !oldIterator.)

		// xy contains the x and y (low and high) evacuation destinations.

		// 默认是等 size 扩容，前后 bucket 序号不变
		var xy [2]evacDst
		// 使用 x 来进行搬迁
		x := &xy[0]
		x.b = (*bmap)(add(h.buckets, oldbucket*uintptr(t.bucketsize)))
		x.k = add(unsafe.Pointer(x.b), dataOffset)
		x.e = add(x.k, bucketCnt*uintptr(t.keysize))

		// 如果不是等 size 扩容，前后 bucket 序号有变
		if !h.sameSizeGrow() {
			// Only calculate y pointers if we're growing bigger.
			// Otherwise GC can see bad pointers.
			// 使用 y 来进行搬迁
			y := &xy[1]
			// y 代表的 bucket 序号增加了 2^B
			y.b = (*bmap)(add(h.buckets, (oldbucket+newbit)*uintptr(t.bucketsize)))
			y.k = add(unsafe.Pointer(y.b), dataOffset)
			y.e = add(y.k, bucketCnt*uintptr(t.keysize))
		}

		// 遍历所有的 bucket，包括 overflow buckets b 是老的 bucket 地址
		for ; b != nil; b = b.overflow(t) {
			k := add(unsafe.Pointer(b), dataOffset)
			e := add(k, bucketCnt*uintptr(t.keysize))
			// 遍历 bucket 中的所有 cell
			for i := 0; i < bucketCnt; i, k, e = i+1, add(k, uintptr(t.keysize)), add(e, uintptr(t.elemsize)) {
				// 当前 cell 的 top hash 值
				top := b.tophash[i]
				// 如果 cell 为空，即没有 key
				if isEmpty(top) {
					// 那就标志它被"搬迁"过
					b.tophash[i] = evacuatedEmpty
					continue
				}
				// 正常不会出现这种情况
				// 未被搬迁的 cell 只可能是 empty 或是
				// 正常的 top hash（大于 minTopHash）
				if top < minTopHash {
					throw("bad map state")
				}
				// 如果 key 是指针，则解引用
				k2 := k
				if t.indirectkey() {
					k2 = *((*unsafe.Pointer)(k2))
				}
				var useY uint8
				// 如果不是等量扩容
				if !h.sameSizeGrow() {
					// Compute hash to make our evacuation decision (whether we need
					// to send this key/elem to bucket x or bucket y).
					// 计算 hash 值，和 key 第一次写入时一样
					hash := t.hasher(k2, uintptr(h.hash0))
					// 如果有协程正在遍历 map 如果出现 相同的 key 值，算出来的 hash 值不同
					if h.flags&iterator != 0 && !t.reflexivekey() && !t.key.equal(k2, k2) {
						// If key != key (NaNs), then the hash could be (and probably
						// will be) entirely different from the old hash. Moreover,
						// it isn't reproducible. Reproducibility is required in the
						// presence of iterators, as our evacuation decision must
						// match whatever decision the iterator made.
						// Fortunately, we have the freedom to send these keys either
						// way. Also, tophash is meaningless for these kinds of keys.
						// We let the low bit of tophash drive the evacuation decision.
						// We recompute a new random tophash for the next level so
						// these keys will get evenly distributed across all buckets
						// after multiple grows.
						// useY =1 使用位置Y
						useY = top & 1
						top = tophash(hash)
					} else {
						// 第 B 位置 不是 0
						if hash&newbit != 0 {
							//使用位置Y
							useY = 1
						}
					}
				}

				if evacuatedX+1 != evacuatedY || evacuatedX^1 != evacuatedY {
					throw("bad evacuatedN")
				}

				//决定key是裂变到 X 还是 Y
				b.tophash[i] = evacuatedX + useY // evacuatedX + 1 == evacuatedY
				dst := &xy[useY]                 // evacuation destination

				// 如果 xi 等于 8，说明要溢出了
				if dst.i == bucketCnt {
					// 新建一个 bucket
					dst.b = h.newoverflow(t, dst.b)
					// xi 从 0 开始计数
					dst.i = 0
					//key移动的位置
					dst.k = add(unsafe.Pointer(dst.b), dataOffset)
					//value 移动的位置
					dst.e = add(dst.k, bucketCnt*uintptr(t.keysize))
				}
				// 设置 top hash 值
				dst.b.tophash[dst.i&(bucketCnt-1)] = top // mask dst.i as an optimization, to avoid a bounds check
				// key 是指针
				if t.indirectkey() {
					// 将原 key（是指针）复制到新位置
					*(*unsafe.Pointer)(dst.k) = k2 // copy pointer
				} else {
					// 将原 key（是值）复制到新位置
					typedmemmove(t.key, dst.k, k) // copy elem
				}
				//value同上
				if t.indirectelem() {
					*(*unsafe.Pointer)(dst.e) = *(*unsafe.Pointer)(e)
				} else {
					typedmemmove(t.elem, dst.e, e)
				}
				// 定位到下一个 cell
				dst.i++
				// These updates might push these pointers past the end of the
				// key or elem arrays.  That's ok, as we have the overflow pointer
				// at the end of the bucket to protect against pointing past the
				// end of the bucket.
				dst.k = add(dst.k, uintptr(t.keysize))
				dst.e = add(dst.e, uintptr(t.elemsize))
			}
		}
		// Unlink the overflow buckets & clear key/elem to help GC.
		// bucket搬迁完毕 如果没有协程在使用老的 buckets，就把老 buckets 清除掉，帮助gc
		if h.flags&oldIterator == 0 && t.bucket.ptrdata != 0 {
			b := add(h.oldbuckets, oldbucket*uintptr(t.bucketsize))
			// Preserve b.tophash because the evacuation
			// state is maintained there.
			ptr := add(b, dataOffset)
			n := uintptr(t.bucketsize) - dataOffset
			memclrHasPointers(ptr, n)
		}
	}

	// 更新搬迁进度
	if oldbucket == h.nevacuate {
		advanceEvacuationMark(h, t, newbit)
	}
}

func advanceEvacuationMark(h *hmap, t *maptype, newbit uintptr) {
	h.nevacuate++
	// Experiments suggest that 1024 is overkill by at least an order of magnitude.
	// Put it in there as a safeguard anyway, to ensure O(1) behavior.
	stop := h.nevacuate + 1024
	if stop > newbit {
		stop = newbit
	}
	for h.nevacuate != stop && bucketEvacuated(t, h, h.nevacuate) {
		h.nevacuate++
	}
	if h.nevacuate == newbit { // newbit == # of oldbuckets
		// Growing is all done. Free old main bucket array.
		h.oldbuckets = nil
		// Can discard old overflow buckets as well.
		// If they are still referenced by an iterator,
		// then the iterator holds a pointers to the slice.
		if h.extra != nil {
			h.extra.oldoverflow = nil
		}
		h.flags &^= sameSizeGrow
	}
}

// Reflect stubs. Called from ../reflect/asm_*.s

//go:linkname reflect_makemap reflect.makemap
func reflect_makemap(t *maptype, cap int) *hmap {
	// Check invariants and reflects math.
	if t.key.equal == nil {
		throw("runtime.reflect_makemap: unsupported map key type")
	}
	if t.key.size > maxKeySize && (!t.indirectkey() || t.keysize != uint8(sys.PtrSize)) ||
		t.key.size <= maxKeySize && (t.indirectkey() || t.keysize != uint8(t.key.size)) {
		throw("key size wrong")
	}
	if t.elem.size > maxElemSize && (!t.indirectelem() || t.elemsize != uint8(sys.PtrSize)) ||
		t.elem.size <= maxElemSize && (t.indirectelem() || t.elemsize != uint8(t.elem.size)) {
		throw("elem size wrong")
	}
	if t.key.align > bucketCnt {
		throw("key align too big")
	}
	if t.elem.align > bucketCnt {
		throw("elem align too big")
	}
	if t.key.size%uintptr(t.key.align) != 0 {
		throw("key size not a multiple of key align")
	}
	if t.elem.size%uintptr(t.elem.align) != 0 {
		throw("elem size not a multiple of elem align")
	}
	if bucketCnt < 8 {
		throw("bucketsize too small for proper alignment")
	}
	if dataOffset%uintptr(t.key.align) != 0 {
		throw("need padding in bucket (key)")
	}
	if dataOffset%uintptr(t.elem.align) != 0 {
		throw("need padding in bucket (elem)")
	}

	return makemap(t, cap, nil)
}

//go:linkname reflect_mapaccess reflect.mapaccess
func reflect_mapaccess(t *maptype, h *hmap, key unsafe.Pointer) unsafe.Pointer {
	elem, ok := mapaccess2(t, h, key)
	if !ok {
		// reflect wants nil for a missing element
		elem = nil
	}
	return elem
}

//go:linkname reflect_mapassign reflect.mapassign
func reflect_mapassign(t *maptype, h *hmap, key unsafe.Pointer, elem unsafe.Pointer) {
	p := mapassign(t, h, key)
	typedmemmove(t.elem, p, elem)
}

//go:linkname reflect_mapdelete reflect.mapdelete
func reflect_mapdelete(t *maptype, h *hmap, key unsafe.Pointer) {
	mapdelete(t, h, key)
}

//go:linkname reflect_mapiterinit reflect.mapiterinit
func reflect_mapiterinit(t *maptype, h *hmap) *hiter {
	it := new(hiter)
	mapiterinit(t, h, it)
	return it
}

//go:linkname reflect_mapiternext reflect.mapiternext
func reflect_mapiternext(it *hiter) {
	mapiternext(it)
}

//go:linkname reflect_mapiterkey reflect.mapiterkey
func reflect_mapiterkey(it *hiter) unsafe.Pointer {
	return it.key
}

//go:linkname reflect_mapiterelem reflect.mapiterelem
func reflect_mapiterelem(it *hiter) unsafe.Pointer {
	return it.elem
}

//go:linkname reflect_maplen reflect.maplen
func reflect_maplen(h *hmap) int {
	if h == nil {
		return 0
	}
	if raceenabled {
		callerpc := getcallerpc()
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(reflect_maplen))
	}
	return h.count
}

//go:linkname reflectlite_maplen internal/reflectlite.maplen
func reflectlite_maplen(h *hmap) int {
	if h == nil {
		return 0
	}
	if raceenabled {
		callerpc := getcallerpc()
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(reflect_maplen))
	}
	return h.count
}

const maxZero = 1024 // must match value in cmd/compile/internal/gc/walk.go:zeroValSize
var zeroVal [maxZero]byte
