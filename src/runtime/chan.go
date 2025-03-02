// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

// This file contains the implementation of Go channels.

// Invariants:
//  At least one of c.sendq and c.recvq is empty,
//  except for the case of an unbuffered channel with a single goroutine
//  blocked on it for both sending and receiving using a select statement,
//  in which case the length of c.sendq and c.recvq is limited only by the
//  size of the select statement.
//
// For buffered channels, also:
//  c.qcount > 0 implies that c.recvq is empty.
//  c.qcount < c.dataqsiz implies that c.sendq is empty.

import (
	"runtime/internal/atomic"
	"runtime/internal/math"
	"unsafe"
)

const (
	maxAlign  = 8
	hchanSize = unsafe.Sizeof(hchan{}) + uintptr(-int(unsafe.Sizeof(hchan{}))&(maxAlign-1))
	debugChan = false
)

type hchan struct {
	// 如果 qcount 和 dataqsiz 的值相同，则表示缓冲区用完了。
	// chan 里元素数量
	qcount uint // total data in the queue						buffer 中已放入的元素个数
	//chan 底层循环数组的长度
	dataqsiz uint // size of the circular queue					用户构造 channel 时指定的 buf 大小
	// 指向底层循环数组的指针
	// 只针对有缓冲的 channel
	buf unsafe.Pointer // points to an array of dataqsiz elements		环形缓冲区

	elemsize uint16 // buffer 中每个元素的大小
	closed   uint32 // channel 是否关闭，== 0 代表未 closed
	elemtype *_type // element type											// channel 元素的类型信息

	// 已发送元素在循环数组中的索引
	sendx uint // send index											buffer 中已发送的索引位置 send index
	// 已接收元素在循环数组中的索引
	recvx uint  // receive index										buffer 中已接收的索引位置 receive index
	recvq waitq // list of recv waiters									等待接收的 goroutine
	sendq waitq // list of send waiters									等待发送的 goroutine

	// lock protects all fields in hchan, as well as several
	// fields in sudogs blocked on this channel.
	//
	// Do not change another G's status while holding this lock
	// (in particular, do not ready a G), as this can deadlock
	// with stack shrinking.
	// lock 用来保证每个读 channel 或写 channel 的操作都是原子的。
	lock mutex
}

type waitq struct {
	first *sudog
	last  *sudog
}

//go:linkname reflect_makechan reflect.makechan
func reflect_makechan(t *chantype, size int) *hchan {
	return makechan(t, size)
}

func makechan64(t *chantype, size int64) *hchan {
	if int64(int(size)) != size {
		panic(plainError("makechan: size out of range"))
	}

	return makechan(t, int(size))
}

// 通过汇编分析，我们知道，最终创建 chan 的函数是 makechan
/*
	使用建议
	小元素优先：避免传递超过1KB的结构体，优先使用指针。
	合理缓冲区：无缓冲channel（size=0）适用于同步场景，缓冲channel需评估容量。
	监控内存：超大缓冲区或高频创建channel可能引发内存问题。
*/
func makechan(t *chantype, size int) *hchan {
	elem := t.elem

	// compiler checks this but be safe.
	/*
		目的：限制channel元素大小不超过 64KB（1<<16字节） 2^16 = 2^10 * 2^6 = 64 * 1024。
		原因：
		过大的元素会导致内存分配效率低下。
		频繁传递大对象会引发性能问题（深拷贝开销）。
		建议改用指针类型（如chan *BigStruct）传递大对象。
	*/
	if elem.size >= 1<<16 {
		throw("makechan: invalid channel element type")
	}
	/*
		目的：确保hchan结构体和元素类型的内存对齐合法。
		关键参数：
		hchanSize：hchan结构体自身的大小（Go版本不同可能变化，通常约96字节）。
		maxAlign：系统最大对齐系数（通常为8或16字节）。
	*/
	if hchanSize%maxAlign != 0 || elem.align > maxAlign {
		throw("makechan: bad alignment")
	}

	/*
		步骤：
		计算缓冲区总内存：元素大小 × 缓冲区长度。
		检查乘法溢出（如size极大导致mem超出uintptr范围）。
		验证总内存不超过系统限制：mem + hchanSize ≤ maxAlloc。
		禁止负数的缓冲区长度（size < 0）。

		关键参数：
		maxAlloc：Go运行时允许的最大堆内存（通常接近系统可用内存）。
		hchanSize：channel元数据占用的内存。
	*/
	mem, overflow := math.MulUintptr(elem.size, uintptr(size))
	if overflow || mem > maxAlloc-hchanSize || size < 0 {
		panic(plainError("makechan: size out of range"))
	}

	// Hchan does not contain pointers interesting for GC when elements stored in buf do not contain pointers.
	// buf points into the same allocation, elemtype is persistent.
	// SudoG's are referenced from their owning thread so they can't be collected.
	// TODO(dvyukov,rlh): Rethink when collector can move allocated objects.
	var c *hchan
	switch {
	case mem == 0:
		// 1. 无缓冲
		// Queue or element size is zero.
		c = (*hchan)(mallocgc(hchanSize, nil, true))
		// Race detector uses this location for synchronization.
		c.buf = c.raceaddr()
	case elem.ptrdata == 0:
		// 2. 有缓冲，不包含指针
		// Elements do not contain pointers.
		// Allocate hchan and buf in one call.
		c = (*hchan)(mallocgc(hchanSize+mem, nil, true))
		c.buf = add(unsafe.Pointer(c), hchanSize)
	default:
		// 3. 有缓冲，包含指针
		// Elements contain pointers.
		c = new(hchan)
		c.buf = mallocgc(mem, elem, true)
	}

	// 用于发送/接收时的内存拷贝
	c.elemsize = uint16(elem.size)
	c.elemtype = elem
	// 决定缓冲区的环形队列容量
	c.dataqsiz = uint(size)
	/*
		锁层级：
		使用lockRankHchan定义锁的获取顺序
		防止与其他系统锁（如GC锁）产生死锁

		并发控制：
		保护sendq/recvq等待队列
		同步缓冲区读写操作
	*/
	lockInit(&c.lock, lockRankHchan)

	if debugChan {
		print("makechan: chan=", c, "; elemsize=", elem.size, "; dataqsiz=", size, "\n")
	}
	return c
}

// chanbuf(c, i) is pointer to the i'th slot in the buffer.
func chanbuf(c *hchan, i uint) unsafe.Pointer {
	return add(c.buf, uintptr(i)*uintptr(c.elemsize))
}

// full reports whether a send on c would block (that is, the channel is full).
// It uses a single word-sized read of mutable state, so although
// the answer is instantaneously true, the correct answer may have changed
// by the time the calling function receives the return value.
func full(c *hchan) bool {
	// c.dataqsiz is immutable (never written after the channel is created)
	// so it is safe to read at any time during channel operation.
	if c.dataqsiz == 0 {
		// Assumes that a pointer read is relaxed-atomic.
		return c.recvq.first == nil
	}
	// Assumes that a uint read is relaxed-atomic.
	return c.qcount == c.dataqsiz
}

// entry point for c <- x from compiled code
//go:nosplit
func chansend1(c *hchan, elem unsafe.Pointer) {
	chansend(c, elem, true, getcallerpc())
}

/*
 * generic single channel send/recv
 * If block is not nil,
 * then the protocol will not
 * sleep but return if it could
 * not complete.
 *
 * sleep can wake up with g.param == nil
 * when a channel involved in the sleep has
 * been closed.  it is easiest to loop and re-run
 * the operation; we'll see that it's now closed.
 */
// channel 发送数据
// 参数 c 表示要向哪个 chan 发送数据，
// ep 表示要发送的数据的地址，
// block 表示是否需要阻塞，
// callerpc 表示调用地址。
// 返回值 bool 表示数据是否成功发送。
/*
	block是谁传的，根据什么决定是否要阻塞？
	block 参数由 编译器根据代码语义自动生成，其值取决于发送操作是否处于 可能非阻塞的上下文 中。以下是具体决策逻辑：
	1. 普通发送操作 (ch <- data): block=true 默认阻塞模式
	2. select 的非阻塞分支: block=false，当 select 包含 default 分支时，所有 case 的发送操作均为非阻塞模式
*/
func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
	if c == nil {
		if !block {
			return false
		}
		gopark(nil, nil, waitReasonChanSendNilChan, traceEvGoStop, 2)
		throw("unreachable")
	}

	if debugChan {
		print("chansend: chan=", c, "\n")
	}
	// 竞态检测支持
	if raceenabled {
		racereadpc(c.raceaddr(), callerpc, funcPC(chansend))
	}

	// Fast path: check for failed non-blocking operation without acquiring the lock.
	//
	// After observing that the channel is not closed, we observe that the channel is
	// not ready for sending. Each of these observations is a single word-sized read
	// (first c.closed and second full()).
	// Because a closed channel cannot transition from 'ready for sending' to
	// 'not ready for sending', even if the channel is closed between the two observations,
	// they imply a moment between the two when the channel was both not yet closed
	// and not ready for sending. We behave as if we observed the channel at that moment,
	// and report that the send cannot proceed.
	//
	// It is okay if the reads are reordered here: if we observe that the channel is not
	// ready for sending and then observe that it is not closed, that implies that the
	// channel wasn't closed during the first observation. However, nothing here
	// guarantees forward progress. We rely on the side effects of lock release in
	// chanrecv() and closechan() to update this thread's view of c.closed and full().
	/*
		三重条件校验：
			非阻塞模式
			channel未关闭
			缓冲区已满（或无缓冲且无等待接收者）
		性能收益：避免锁竞争，提升高频操作性能
		内存序保证：依赖c.closed和full()的原子读取顺序
	*/
	if !block && c.closed == 0 && full(c) {
		return false
	}

	var t0 int64
	if blockprofilerate > 0 {
		t0 = cputicks()
	}

	// 1. 通道加锁
	lock(&c.lock)

	if c.closed != 0 {
		unlock(&c.lock)
		panic(plainError("send on closed channel"))
	}

	// 2. 发送数据
	// 2.1 如果此时 recvq 不为空，说明有G在阻塞等待，则调用 send 函数将数据拷贝到对应的 G 的堆栈上，然后将其唤醒。
	if sg := c.recvq.dequeue(); sg != nil {
		// Found a waiting receiver. We pass the value we want to send
		// directly to the receiver, bypassing the channel buffer (if any).
		/*
			绕过缓冲区直接内存拷贝
			执行异步解锁（通过func() { unlock(&c.lock) }）

			send函数关键操作：
				调用memmove拷贝数据到接收方内存
				设置sg.g.param为发送数据地址
				通过goready唤醒接收goroutine
		*/
		send(c, sg, ep, func() { unlock(&c.lock) }, 3)
		return true
	}

	// 2.2 如果缓冲区没满，channel 会尝试把数据放到缓存中。
	/*
		内存布局：缓冲区作为连续内存块，通过chanbuf计算偏移
		类型安全：typedmemmove根据元素类型执行拷贝（处理指针等）
	*/
	if c.qcount < c.dataqsiz {
		// Space is available in the channel buffer. Enqueue the element to send.

		// chanbuf 函数从 buf 中取出第 i 个元素的存放地址：相当于 c.buf[c.sendx]
		qp := chanbuf(c, c.sendx)
		if raceenabled {
			raceacquire(qp)
			racerelease(qp)
		}
		// 将数据拷贝到 buffer 中
		typedmemmove(c.elemtype, qp, ep)
		// 写入的index + 1
		c.sendx++
		if c.sendx == c.dataqsiz {
			c.sendx = 0
		}
		// buffer中的元素总数+1
		c.qcount++
		unlock(&c.lock)
		return true
	}

	// 非阻塞，这里直接返回
	if !block {
		unlock(&c.lock)
		return false
	}

	// 2.3 如果没有缓冲区，或者缓冲区已满。
	// 此时会将当前的 goroutine 以及要发送的数据放入到 sendq 队列中，同时会切出该 goroutine。
	// Block on the channel. Some receiver will complete our operation for us.
	// 获取当前G
	gp := getg()
	// acquireSudog 申请一个 sudog 对象。
	mysg := acquireSudog()
	mysg.releasetime = 0
	if t0 != 0 {
		mysg.releasetime = -1
	}
	// No stack splits between assigning elem and enqueuing mysg
	// on gp.waiting where copystack can find it.
	// 绑定发送数据地址
	mysg.elem = ep
	mysg.waitlink = nil
	// 关联当前goroutine
	mysg.g = gp
	mysg.isSelect = false
	mysg.c = c
	gp.waiting = mysg
	gp.param = nil
	// 封装好的当前G放到发送等待列队
	c.sendq.enqueue(mysg)
	// goparkunlock 就是解锁传入的 mutex，并切出该 goroutine，将该 goroutine 置为 waiting 状态。
	/*
		gopark流程：
			切换goroutine状态为_Gwaiting
			执行chanparkcommit回调（标记栈不可收缩）
			释放锁并通过mcall切换到调度器
	*/
	gopark(chanparkcommit, unsafe.Pointer(&c.lock), waitReasonChanSend, traceEvGoBlockSend, 2)

	// 发送数据的协程被唤醒
	/*
		我们可以发现，g被唤醒后并没有继续往channel中发送数据，这是为什么？

		在 Go 语言中，当发送协程因 channel 阻塞被唤醒后，数据实际上已在阻塞期间被接收方处理。
		唤醒后的操作仅负责状态确认和资源清理，而非重新发送数据。这是由 Go 运行时的高效设计决定的，具体机制如下：
		1. 阻塞期间的异步拷贝
			发送方：将数据指针存入 sudog.elem
			接收方：从 sudog.elem 直接拷贝数据到目标地址
			无需唤醒后操作：数据传递在接收方处理时已完成

		2. gp.param 的作用：
			非 nil：接收方已成功拷贝数据
			nil：通道被关闭或出现异常

		1. 正常唤醒（发送成功）
			触发条件：接收方完成数据拷贝
			执行动作：
				解除协程与 channel 的绑定 (gp.waiting = nil)
				标记栈收缩安全 (gp.activeStackChans = false)
				释放 sudog 资源 (releaseSudog)
		2. 异常唤醒
			通道关闭：触发 panic("send on closed channel")
			虚假唤醒：抛出运行时异常 (throw)

		设计优势
			1. 零重复操作
				数据单次传递：避免重复拷贝，提升性能
				无锁优化：接收方直接操作发送方的数据指针
			2. 状态驱动
				原子状态标记：通过 gp.param 明确操作结果
				资源即时回收：快速释放 sudog 减少内存占用
	*/

	// Ensure the value being sent is kept alive until the
	// receiver copies it out. The sudog has a pointer to the
	// stack object, but sudogs aren't considered as roots of the
	// stack tracer.
	KeepAlive(ep)

	/*
		检测目标：确保唤醒的sudog与记录的等待对象一致
	*/
	// someone woke us up.
	if mysg != gp.waiting {
		throw("G waiting list is corrupted")
	}
	// 正常唤醒：接收方通过gp.param传递数据地址（非nil）
	gp.waiting = nil
	// 标记期间禁止栈收缩，防止移动包含channel操作的栈内存
	gp.activeStackChans = false
	// 关闭唤醒，param == nil
	if gp.param == nil {
		if c.closed == 0 {
			throw("chansend: spurious wakeup")
		}
		// 如果阻塞发送g被唤醒后，发现Channel已经关闭，会panic
		panic(plainError("send on closed channel"))
	}
	// 正常唤醒，param != nil
	gp.param = nil
	if mysg.releasetime > 0 {
		blockevent(mysg.releasetime-t0, 2)
	}
	mysg.c = nil
	// releaseSudog()将sudog放回per-P缓存池（非GC回收）
	releaseSudog(mysg)
	return true
}

// send processes a send operation on an empty channel c.
// The value ep sent by the sender is copied to the receiver sg.
// The receiver is then woken up to go on its merry way.
// Channel c must be empty and locked.  send unlocks c with unlockf.
// sg must already be dequeued from c.
// ep must be non-nil and point to the heap or the caller's stack.
func send(c *hchan, sg *sudog, ep unsafe.Pointer, unlockf func(), skip int) {
	if raceenabled {
		if c.dataqsiz == 0 {
			racesync(c, sg)
		} else {
			// Pretend we go through the buffer, even though
			// we copy directly. Note that we need to increment
			// the head/tail locations only when raceenabled.
			qp := chanbuf(c, c.recvx)
			raceacquire(qp)
			racerelease(qp)
			raceacquireg(sg.g, qp)
			racereleaseg(sg.g, qp)
			c.recvx++
			if c.recvx == c.dataqsiz {
				c.recvx = 0
			}
			c.sendx = c.recvx // c.sendx = (c.sendx+1) % c.dataqsiz
		}
	}
	if sg.elem != nil {
		// 1. memmove(dst, src, t.size) 进行数据的转移，本质上就是一个内存拷贝。
		sendDirect(c.elemtype, sg, ep)
		sg.elem = nil
	}
	gp := sg.g
	unlockf()
	gp.param = unsafe.Pointer(sg)
	if sg.releasetime != 0 {
		sg.releasetime = cputicks()
	}
	// 2. goready(gp, skip+1) goready 的作用是唤醒对应的 goroutine。
	goready(gp, skip+1)
}

// Sends and receives on unbuffered or empty-buffered channels are the
// only operations where one running goroutine writes to the stack of
// another running goroutine. The GC assumes that stack writes only
// happen when the goroutine is running and are only done by that
// goroutine. Using a write barrier is sufficient to make up for
// violating that assumption, but the write barrier has to work.
// typedmemmove will call bulkBarrierPreWrite, but the target bytes
// are not in the heap, so that will not help. We arrange to call
// memmove and typeBitsBulkBarrier instead.

// 向一个非缓冲型的 channel 发送数据、从一个无元素的（非缓冲型或缓冲型但空）的 channel
// 接收数据，都会导致一个 goroutine 直接操作另一个 goroutine 的栈
// 由于 GC 假设对栈的写操作只能发生在 goroutine 正在运行中并且由当前 goroutine 来写
// 所以这里实际上违反了这个假设。可能会造成一些问题，所以需要用到写屏障来规避
/*
	这里涉及到一个 goroutine 直接写另一个 goroutine 栈的操作，一般而言，不同 goroutine 的栈是各自独有的。
	而这也违反了 GC 的一些假设。为了不出问题，写的过程中增加了写屏障，保证正确地完成写操作。
	这样做的好处是减少了一次内存 copy：不用先拷贝到 channel 的 buf，直接由发送者到接收者，没有中间商赚差价，效率得以提高，完美。
*/
func sendDirect(t *_type, sg *sudog, src unsafe.Pointer) {
	// src is on our stack, dst is a slot on another stack.

	// Once we read sg.elem out of sg, it will no longer
	// be updated if the destination's stack gets copied (shrunk).
	// So make sure that no preemption points can happen between read & use.

	// src 在当前 goroutine 的栈上，dst 是另一个 goroutine 的栈

	// 直接进行内存"搬迁"
	// 如果目标地址的栈发生了栈收缩，当我们读出了 sg.elem 后
	// 就不能修改真正的 dst 位置的值了
	// 因此需要在读和写之前加上一个屏障
	dst := sg.elem
	typeBitsBulkBarrier(t, uintptr(dst), uintptr(src), t.size)
	// No need for cgo write barrier checks because dst is always
	// Go memory.
	memmove(dst, src, t.size)
}

func recvDirect(t *_type, sg *sudog, dst unsafe.Pointer) {
	// dst is on our stack or the heap, src is on another stack.
	// The channel is locked, so src will not move during this
	// operation.
	src := sg.elem
	typeBitsBulkBarrier(t, uintptr(dst), uintptr(src), t.size)
	memmove(dst, src, t.size)
}

func closechan(c *hchan) {
	if c == nil {
		panic(plainError("close of nil channel"))
	}

	// 1. 加锁
	lock(&c.lock)
	if c.closed != 0 {
		unlock(&c.lock)
		panic(plainError("close of closed channel"))
	}

	if raceenabled {
		callerpc := getcallerpc()
		racewritepc(c.raceaddr(), callerpc, funcPC(closechan))
		racerelease(c.raceaddr())
	}

	// 2. 将锁标志置为1
	c.closed = 1

	var glist gList

	// release all readers
	for {
		// 3. 唤醒所有的接收者，并且将接收数据置为 0 值。
		sg := c.recvq.dequeue()
		if sg == nil {
			break
		}
		// 如果 elem 不为空，说明此 receiver 未忽略接收数据
		// 给它赋一个相应类型的零值
		if sg.elem != nil {
			typedmemclr(c.elemtype, sg.elem)
			sg.elem = nil
		}
		if sg.releasetime != 0 {
			sg.releasetime = cputicks()
		}
		gp := sg.g
		// 将gp.param设置为nil，标识该g是channel关闭时被唤醒
		gp.param = nil
		if raceenabled {
			raceacquireg(gp, c.raceaddr())
		}
		glist.push(gp)
	}

	// release all writers (they will panic)
	for {
		// 4. 唤醒所有发送者，令其 panic。
		sg := c.sendq.dequeue()
		if sg == nil {
			break
		}
		sg.elem = nil
		if sg.releasetime != 0 {
			sg.releasetime = cputicks()
		}
		gp := sg.g
		// 将gp.param设置为nil，标识该g是channel关闭时被唤醒
		gp.param = nil
		if raceenabled {
			raceacquireg(gp, c.raceaddr())
		}
		glist.push(gp)
	}
	unlock(&c.lock)

	// Ready all Gs now that we've dropped the channel lock.
	// 将所有等待接受数据的g和等待发送数据的g唤醒
	for !glist.empty() {
		gp := glist.pop()
		gp.schedlink = 0
		goready(gp, 3)
	}
}

// empty reports whether a read from c would block (that is, the channel is
// empty).  It uses a single atomic read of mutable state.
func empty(c *hchan) bool {
	// c.dataqsiz is immutable.
	if c.dataqsiz == 0 {
		return atomic.Loadp(unsafe.Pointer(&c.sendq.first)) == nil
	}
	return atomic.Loaduint(&c.qcount) == 0
}

// entry points for <- c from compiled code
//go:nosplit
func chanrecv1(c *hchan, elem unsafe.Pointer) {
	chanrecv(c, elem, true)
}

//go:nosplit
func chanrecv2(c *hchan, elem unsafe.Pointer) (received bool) {
	_, received = chanrecv(c, elem, true)
	return
}

// chanrecv receives on channel c and writes the received data to ep.
// ep may be nil, in which case received data is ignored.
// If block == false and no elements are available, returns (false, false).
// Otherwise, if c is closed, zeros *ep and returns (true, false).
// Otherwise, fills in *ep with an element and returns (true, true).
// A non-nil ep must point to the heap or the caller's stack.
// channel 接收数据
/*
	参数说明：
	c *hchan：目标 channel 的指针
	ep unsafe.Pointer：接收数据的内存地址（可为 nil，表示忽略数据）
	block bool：是否阻塞模式（由编译器根据代码上下文决定）普通接受默认阻塞、当 select 包含 default 分支时，所有 case 的接收操作均为非阻塞模式

	返回值：
	selected：是否成功执行接收操作（包括从关闭的 channel 接收）
	received：是否实际获取到数据（若 channel 关闭且无数据，返回 false）
*/
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
	// raceenabled: don't need to check ep, as it is always on the stack
	// or is new memory allocated by reflect.

	if debugChan {
		print("chanrecv: chan=", c, "\n")
	}

	/*
		行为差异：
		非阻塞模式：立即返回 (false, false)
		阻塞模式：协程永久挂起（Go 的设计哲学：操作 nil channel 是逻辑错误）
	*/
	if c == nil {
		if !block {
			return
		}
		gopark(nil, nil, waitReasonChanReceiveNilChan, traceEvGoStop, 2)
		throw("unreachable")
	}

	// 快速失败路径
	// Fast path: check for failed non-blocking operation without acquiring the lock.
	if !block && empty(c) {
		// After observing that the channel is not ready for receiving, we observe whether the
		// channel is closed.
		//
		// Reordering of these checks could lead to incorrect behavior when racing with a close.
		// For example, if the channel was open and not empty, was closed, and then drained,
		// reordered reads could incorrectly indicate "open and empty". To prevent reordering,
		// we use atomic loads for both checks, and rely on emptying and closing to happen in
		// separate critical sections under the same lock.  This assumption fails when closing
		// an unbuffered channel with a blocked send, but that is an error condition anyway.
		if atomic.Load(&c.closed) == 0 {
			// Because a channel cannot be reopened, the later observation of the channel
			// being not closed implies that it was also not closed at the moment of the
			// first observation. We behave as if we observed the channel at that moment
			// and report that the receive cannot proceed.
			return
		}
		// The channel is irreversibly closed. Re-check whether the channel has any pending data
		// to receive, which could have arrived between the empty and closed checks above.
		// Sequential consistency is also required here, when racing with such a send.
		if empty(c) {
			// The channel is irreversibly closed and empty.
			if raceenabled {
				raceacquire(c.raceaddr())
			}
			if ep != nil {
				typedmemclr(c.elemtype, ep)
			}
			return true, false
		}
	}

	var t0 int64
	if blockprofilerate > 0 {
		t0 = cputicks()
	}

	lock(&c.lock)

	if c.closed != 0 && c.qcount == 0 {
		if raceenabled {
			raceacquire(c.raceaddr())
		}
		unlock(&c.lock)
		if ep != nil {
			typedmemclr(c.elemtype, ep)
		}
		return true, false
	}

	/*
		1. 没有缓冲区：直接接收发送者的数据
		2. 缓冲区有数据:	优先消费缓冲区队首元素（FIFO 原则）
		3. 缓冲区空且有等待发送者：直接接收发送者的数据（绕过缓冲区）
	*/
	if sg := c.sendq.dequeue(); sg != nil {
		// 进入这里有两种情况：
		// 1. channel 没有缓冲区
		// 2. channel 有缓冲区且已经满了
		// Found a waiting sender. If buffer is size 0, receive value
		// directly from sender. Otherwise, receive from head of queue
		// and add sender's value to the tail of the queue (both map to
		// the same buffer slot because the queue is full).
		recv(c, sg, ep, func() { unlock(&c.lock) }, 3)
		return true, true
	}

	// 剩余情况： 3. channel有缓冲区且缓冲区未满。
	// 还是从缓冲区取数据，但是没有阻塞的发送g，所有需要处理等待发送者。
	if c.qcount > 0 {
		// Receive directly from queue
		qp := chanbuf(c, c.recvx)
		if raceenabled {
			raceacquire(qp)
			racerelease(qp)
		}
		if ep != nil {
			typedmemmove(c.elemtype, ep, qp)
		}
		typedmemclr(c.elemtype, qp)
		c.recvx++
		if c.recvx == c.dataqsiz {
			c.recvx = 0
		}
		c.qcount--
		unlock(&c.lock)
		return true, true
	}

	// 剩余情况：4. channel有缓冲区且缓冲区为空
	if !block {
		unlock(&c.lock)
		return false, false
	}

	// no sender available: block on this channel.
	gp := getg()
	mysg := acquireSudog()
	mysg.releasetime = 0
	if t0 != 0 {
		mysg.releasetime = -1
	}
	// No stack splits between assigning elem and enqueuing mysg
	// on gp.waiting where copystack can find it.
	mysg.elem = ep
	mysg.waitlink = nil
	gp.waiting = mysg
	mysg.g = gp
	mysg.isSelect = false
	mysg.c = c
	gp.param = nil
	// 发送者被封装成 mysg，放入等待发送队列
	c.recvq.enqueue(mysg)
	// 当前g阻塞
	gopark(chanparkcommit, unsafe.Pointer(&c.lock), waitReasonChanReceive, traceEvGoBlockRecv, 2)

	// 阻塞的发送g被唤醒，也是只需要做些资源释放工作。
	// 因为要接受的数据，已经在发送g中，直接通过内存复制的方式，复制到ep指向的内存地址了。
	// someone woke us up
	if mysg != gp.waiting {
		throw("G waiting list is corrupted")
	}
	gp.waiting = nil
	gp.activeStackChans = false
	if mysg.releasetime > 0 {
		blockevent(mysg.releasetime-t0, 2)
	}
	closed := gp.param == nil
	gp.param = nil
	mysg.c = nil
	releaseSudog(mysg)
	return true, !closed
}

// recv processes a receive operation on a full channel c.
// There are 2 parts:
// 1) The value sent by the sender sg is put into the channel
//    and the sender is woken up to go on its merry way.
// 2) The value received by the receiver (the current G) is
//    written to ep.
// For synchronous channels, both values are the same.
// For asynchronous channels, the receiver gets its data from
// the channel buffer and the sender's data is put in the
// channel buffer.
// Channel c must be full and locked. recv unlocks c with unlockf.
// sg must already be dequeued from c.
// A non-nil ep must point to the heap or the caller's stack.
func recv(c *hchan, sg *sudog, ep unsafe.Pointer, unlockf func(), skip int) {
	if c.dataqsiz == 0 {
		// 1. 如果 c.dataqsiz 为 0，表示通道是无缓冲的（synchronous channel）。
		// 在这种情况下，直接将发送者的数据复制到接收者的存储位置 ep。
		if raceenabled {
			racesync(c, sg)
		}
		if ep != nil {
			// copy data from sender
			recvDirect(c.elemtype, sg, ep)
		}
	} else {
		// Queue is full. Take the item at the
		// head of the queue. Make the sender enqueue
		// its item at the tail of the queue. Since the
		// queue is full, those are both the same slot.
		// 2. 如果通道是有缓冲的（asynchronous channel），并且已经满了，函数会执行以下步骤：
		// 2.1 获取通道缓冲区中接收位置的指针 qp。
		qp := chanbuf(c, c.recvx)
		if raceenabled {
			raceacquire(qp)
			racerelease(qp)
			raceacquireg(sg.g, qp)
			racereleaseg(sg.g, qp)
		}
		// copy data from queue to receiver
		// 2.2 将缓冲区中的数据复制到接收者的存储位置 ep。
		if ep != nil {
			typedmemmove(c.elemtype, ep, qp)
		}
		// copy data from sender to queue
		// 2.3 将发送者的数据复制到缓冲区的相同位置
		// 由此可见，阻塞在发送队列的g所要发送的数据，是在数据接收的时候被复制到缓冲区的。
		// 所以，阻塞的发送g在被唤醒是，不需要在处理它要发送的数据，只需要做些资源释放工作。
		typedmemmove(c.elemtype, qp, sg.elem)
		// 2.4 更新接收位置 recvx 和发送位置 sendx，确保它们在缓冲区的范围内循环。
		c.recvx++
		if c.recvx == c.dataqsiz {
			c.recvx = 0
		}
		c.sendx = c.recvx // c.sendx = (c.sendx+1) % c.dataqsiz
	}
	// 将发送者的元素指针 sg.elem 设置为 nil，以避免悬空指针。sg.elem的数据已经被复制到了缓冲区。
	sg.elem = nil
	// 获取发送者的 goroutine 指针 gp，并解锁通道。
	gp := sg.g
	unlockf()
	// 将发送者的参数设置为指向 sg 的指针。用gp.param是否为空，来区分阻塞的发送g是从哪里被唤醒的。
	gp.param = unsafe.Pointer(sg)
	if sg.releasetime != 0 {
		sg.releasetime = cputicks()
	}
	// 将发送者的 goroutine 置为就绪状态，准备继续执行。
	goready(gp, skip+1)
}

func chanparkcommit(gp *g, chanLock unsafe.Pointer) bool {
	// There are unlocked sudogs that point into gp's stack. Stack
	// copying must lock the channels of those sudogs.
	gp.activeStackChans = true
	unlock((*mutex)(chanLock))
	return true
}

// compiler implements
//
//	select {
//	case c <- v:
//		... foo
//	default:
//		... bar
//	}
//
// as
//
//	if selectnbsend(c, v) {
//		... foo
//	} else {
//		... bar
//	}
//
func selectnbsend(c *hchan, elem unsafe.Pointer) (selected bool) {
	return chansend(c, elem, false, getcallerpc())
}

// compiler implements
//
//	select {
//	case v = <-c:
//		... foo
//	default:
//		... bar
//	}
//
// as
//
//	if selectnbrecv(&v, c) {
//		... foo
//	} else {
//		... bar
//	}
//
func selectnbrecv(elem unsafe.Pointer, c *hchan) (selected bool) {
	selected, _ = chanrecv(c, elem, false)
	return
}

// compiler implements
//
//	select {
//	case v, ok = <-c:
//		... foo
//	default:
//		... bar
//	}
//
// as
//
//	if c != nil && selectnbrecv2(&v, &ok, c) {
//		... foo
//	} else {
//		... bar
//	}
//
func selectnbrecv2(elem unsafe.Pointer, received *bool, c *hchan) (selected bool) {
	// TODO(khr): just return 2 values from this function, now that it is in Go.
	selected, *received = chanrecv(c, elem, false)
	return
}

//go:linkname reflect_chansend reflect.chansend
func reflect_chansend(c *hchan, elem unsafe.Pointer, nb bool) (selected bool) {
	return chansend(c, elem, !nb, getcallerpc())
}

//go:linkname reflect_chanrecv reflect.chanrecv
func reflect_chanrecv(c *hchan, nb bool, elem unsafe.Pointer) (selected bool, received bool) {
	return chanrecv(c, elem, !nb)
}

//go:linkname reflect_chanlen reflect.chanlen
func reflect_chanlen(c *hchan) int {
	if c == nil {
		return 0
	}
	return int(c.qcount)
}

//go:linkname reflectlite_chanlen internal/reflectlite.chanlen
func reflectlite_chanlen(c *hchan) int {
	if c == nil {
		return 0
	}
	return int(c.qcount)
}

//go:linkname reflect_chancap reflect.chancap
func reflect_chancap(c *hchan) int {
	if c == nil {
		return 0
	}
	return int(c.dataqsiz)
}

//go:linkname reflect_chanclose reflect.chanclose
func reflect_chanclose(c *hchan) {
	closechan(c)
}

func (q *waitq) enqueue(sgp *sudog) {
	sgp.next = nil
	x := q.last
	if x == nil {
		sgp.prev = nil
		q.first = sgp
		q.last = sgp
		return
	}
	sgp.prev = x
	x.next = sgp
	q.last = sgp
}

func (q *waitq) dequeue() *sudog {
	for {
		sgp := q.first
		if sgp == nil {
			return nil
		}
		y := sgp.next
		if y == nil {
			q.first = nil
			q.last = nil
		} else {
			y.prev = nil
			q.first = y
			sgp.next = nil // mark as removed (see dequeueSudog)
		}

		// if a goroutine was put on this queue because of a
		// select, there is a small window between the goroutine
		// being woken up by a different case and it grabbing the
		// channel locks. Once it has the lock
		// it removes itself from the queue, so we won't see it after that.
		// We use a flag in the G struct to tell us when someone
		// else has won the race to signal this goroutine but the goroutine
		// hasn't removed itself from the queue yet.
		if sgp.isSelect && !atomic.Cas(&sgp.g.selectDone, 0, 1) {
			continue
		}

		return sgp
	}
}

func (c *hchan) raceaddr() unsafe.Pointer {
	// Treat read-like and write-like operations on the channel to
	// happen at this address. Avoid using the address of qcount
	// or dataqsiz, because the len() and cap() builtins read
	// those addresses, and we don't want them racing with
	// operations like close().
	return unsafe.Pointer(&c.buf)
}

func racesync(c *hchan, sg *sudog) {
	racerelease(chanbuf(c, 0))
	raceacquireg(sg.g, chanbuf(c, 0))
	racereleaseg(sg.g, chanbuf(c, 0))
	raceacquire(chanbuf(c, 0))
}
