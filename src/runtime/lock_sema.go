// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build aix darwin netbsd openbsd plan9 solaris windows

package runtime

import (
	"runtime/internal/atomic"
	"unsafe"
)

// This implementation depends on OS-specific implementations of
//
//	func semacreate(mp *m)
//		Create a semaphore for mp, if it does not already have one.
//
//	func semasleep(ns int64) int32
//		If ns < 0, acquire m's semaphore and return 0.
//		If ns >= 0, try to acquire m's semaphore for at most ns nanoseconds.
//		Return 0 if the semaphore was acquired, -1 if interrupted or timed out.
//
//	func semawakeup(mp *m)
//		Wake up mp, which is or will soon be sleeping on its semaphore.
//
const (
	locked uintptr = 1

	active_spin     = 4
	active_spin_cnt = 30
	passive_spin    = 1
)

func lock(l *mutex) {
	lockWithRank(l, getLockRank(l))
}

func lock2(l *mutex) {
	gp := getg()
	if gp.m.locks < 0 {
		throw("runtime·lock: lock count")
	}
	gp.m.locks++

	// Speculative grab for lock.
	if atomic.Casuintptr(&l.key, 0, locked) {
		return
	}
	semacreate(gp.m)

	// On uniprocessor's, no point spinning.
	// On multiprocessors, spin for ACTIVE_SPIN attempts.
	spin := 0
	if ncpu > 1 {
		spin = active_spin
	}
Loop:
	for i := 0; ; i++ {
		v := atomic.Loaduintptr(&l.key)
		if v&locked == 0 {
			// Unlocked. Try to lock.
			if atomic.Casuintptr(&l.key, v, v|locked) {
				return
			}
			i = 0
		}
		if i < spin {
			procyield(active_spin_cnt)
		} else if i < spin+passive_spin {
			osyield()
		} else {
			// Someone else has it.
			// l->waitm points to a linked list of M's waiting
			// for this lock, chained through m->nextwaitm.
			// Queue this M.
			for {
				gp.m.nextwaitm = muintptr(v &^ locked)
				if atomic.Casuintptr(&l.key, v, uintptr(unsafe.Pointer(gp.m))|locked) {
					break
				}
				v = atomic.Loaduintptr(&l.key)
				if v&locked == 0 {
					continue Loop
				}
			}
			if v&locked != 0 {
				// Queued. Wait.
				semasleep(-1)
				i = 0
			}
		}
	}
}

func unlock(l *mutex) {
	unlockWithRank(l)
}

//go:nowritebarrier
// We might not be holding a p in this code.
func unlock2(l *mutex) {
	gp := getg()
	var mp *m
	for {
		v := atomic.Loaduintptr(&l.key)
		if v == locked {
			if atomic.Casuintptr(&l.key, locked, 0) {
				break
			}
		} else {
			// Other M's are waiting for the lock.
			// Dequeue an M.
			mp = muintptr(v &^ locked).ptr()
			if atomic.Casuintptr(&l.key, v, uintptr(mp.nextwaitm)) {
				// Dequeued an M.  Wake it.
				semawakeup(mp)
				break
			}
		}
	}
	gp.m.locks--
	if gp.m.locks < 0 {
		throw("runtime·unlock: lock count")
	}
	if gp.m.locks == 0 && gp.preempt { // restore the preemption request in case we've cleared it in newstack
		gp.stackguard0 = stackPreempt
	}
}

// One-time notifications.
func noteclear(n *note) {
	if GOOS == "aix" {
		// On AIX, semaphores might not synchronize the memory in some
		// rare cases. See issue #30189.
		atomic.Storeuintptr(&n.key, 0)
	} else {
		n.key = 0
	}
}

// notewakeup 函数的目的是唤醒等待在 note 上的 M（线程）。
func notewakeup(n *note) {
	var v uintptr
	for {
		v = atomic.Loaduintptr(&n.key)
		// 使用原子比较并交换操作 atomic.Casuintptr 尝试将 n.key 从 v 设置为 locked。locked != 0。
		// notesleep 函数是通过判断 n.key == 0 来继续休眠M的，当n.key ！= 0，notesleep 函数则会跳出休眠的循环。
		if atomic.Casuintptr(&n.key, v, locked) {
			break
		}
	}

	// 成功将 waitm 设置为 locked 后，检查 note 之前的状态 v。
	// Successfully set waitm to locked.
	// What was it before?
	switch {
	case v == 0:
		// 如果 v 为 0，表示没有线程在等待，操作完成。
		// Nothing was waiting. Done.
	case v == locked:
		// 如果 v 为 locked，表示发生了两次 notewakeup，这是不允许的，抛出异常 throw("notewakeup - double wakeup")。
		// Two notewakeups! Not allowed.
		throw("notewakeup - double wakeup")
	default:
		// Must be the waiting m. Wake it up.
		// 否则，v 必须是等待的 M，将其转换为 *m 类型并调用 semawakeup 函数唤醒它。
		// semawakeup 函数用于唤醒等待的 M。具体实现依赖于操作系统的信号量机制。即给M发信号唤醒它。
		semawakeup((*m)(unsafe.Pointer(v)))
	}
}

// notesleep 函数的主要功能是使当前 M 进入休眠状态，直到被唤醒。
// 它使用了 Go 运行时的信号量和原子操作来管理休眠和唤醒的机制。
// 这里找源码需要注意一下: 想看 linux 源码的，就得跳转到 src/runtime/lock_futex.go 139
func notesleep(n *note) {
	// 函数的参数 n 是一个指向 note 结构的指针，表示当前 goroutine 的休眠状态。
	gp := getg()
	// notesleep 只能在 G0 上调用，以确保安全性。
	if gp != gp.m.g0 {
		throw("notesleep not on g0")
	}
	// 调用相关的C函数，创建一个信号量，用于管理当前 M 的休眠状态。
	semacreate(gp.m)
	// 使用 atomic.Casuintptr 原子地将 n.key 的值从 0 更改为当前 M 的指针。如果成功，表示当前 M 已经被排队等待休眠。
	if !atomic.Casuintptr(&n.key, 0, uintptr(unsafe.Pointer(gp.m))) {
		// Must be locked (got wakeup).
		if n.key != locked {
			throw("notesleep - waitm out of sync")
		}
		return
	}
	// Queued. Sleep.
	// 设置 gp.m.blocked = true，表示当前 M 正在被阻塞。
	gp.m.blocked = true
	if *cgo_yield == nil {
		// 调用 semasleep(-1) 使当前 M 进入无限期休眠，直到被唤醒。
		semasleep(-1)
	} else {
		// Sleep for an arbitrary-but-moderate interval to poll libc interceptors.
		// 进入一个循环，定期调用 semasleep(ns) 进行短暂休眠（ns 为 10 微秒），并在每次循环中调用 asmcgocall(*cgo_yield, nil)，以便在休眠期间轮询 Cgo 拦截器。
		// 直到 n.key 被notewakeup函数更改，atomic.Loaduintptr(&n.key)将不为0
		const ns = 10e6
		for atomic.Loaduintptr(&n.key) == 0 {
			semasleep(ns)
			// 切换的系统栈，并执行cgo_yield函数
			// cgo_yield函数的作用是，在休眠期间轮询 Cgo 拦截器
			// 这是为了确保在与 C 代码交互时，Go 运行时能够保持响应性，避免在 C 代码中长时间阻塞。
			asmcgocall(*cgo_yield, nil)
		}
	}
	// 在 M 被唤醒后，gp.m.blocked 被设置为 false，表示当前 M 不再被阻塞。
	// 这是一个重要的步骤，因为它允许调度器知道当前 M 可以再次参与调度。
	gp.m.blocked = false
}

//go:nosplit
func notetsleep_internal(n *note, ns int64, gp *g, deadline int64) bool {
	// gp and deadline are logically local variables, but they are written
	// as parameters so that the stack space they require is charged
	// to the caller.
	// This reduces the nosplit footprint of notetsleep_internal.
	gp = getg()

	// Register for wakeup on n->waitm.
	if !atomic.Casuintptr(&n.key, 0, uintptr(unsafe.Pointer(gp.m))) {
		// Must be locked (got wakeup).
		if n.key != locked {
			throw("notetsleep - waitm out of sync")
		}
		return true
	}
	if ns < 0 {
		// Queued. Sleep.
		gp.m.blocked = true
		if *cgo_yield == nil {
			semasleep(-1)
		} else {
			// Sleep in arbitrary-but-moderate intervals to poll libc interceptors.
			const ns = 10e6
			for semasleep(ns) < 0 {
				asmcgocall(*cgo_yield, nil)
			}
		}
		gp.m.blocked = false
		return true
	}

	deadline = nanotime() + ns
	for {
		// Registered. Sleep.
		gp.m.blocked = true
		if *cgo_yield != nil && ns > 10e6 {
			ns = 10e6
		}
		if semasleep(ns) >= 0 {
			gp.m.blocked = false
			// Acquired semaphore, semawakeup unregistered us.
			// Done.
			return true
		}
		if *cgo_yield != nil {
			asmcgocall(*cgo_yield, nil)
		}
		gp.m.blocked = false
		// Interrupted or timed out. Still registered. Semaphore not acquired.
		ns = deadline - nanotime()
		if ns <= 0 {
			break
		}
		// Deadline hasn't arrived. Keep sleeping.
	}

	// Deadline arrived. Still registered. Semaphore not acquired.
	// Want to give up and return, but have to unregister first,
	// so that any notewakeup racing with the return does not
	// try to grant us the semaphore when we don't expect it.
	for {
		v := atomic.Loaduintptr(&n.key)
		switch v {
		case uintptr(unsafe.Pointer(gp.m)):
			// No wakeup yet; unregister if possible.
			if atomic.Casuintptr(&n.key, v, 0) {
				return false
			}
		case locked:
			// Wakeup happened so semaphore is available.
			// Grab it to avoid getting out of sync.
			gp.m.blocked = true
			if semasleep(-1) < 0 {
				throw("runtime: unable to acquire - semaphore out of sync")
			}
			gp.m.blocked = false
			return true
		default:
			throw("runtime: unexpected waitm - semaphore out of sync")
		}
	}
}

func notetsleep(n *note, ns int64) bool {
	gp := getg()
	if gp != gp.m.g0 {
		throw("notetsleep not on g0")
	}
	semacreate(gp.m)
	return notetsleep_internal(n, ns, nil, 0)
}

// same as runtime·notetsleep, but called on user g (not g0)
// calls only nosplit functions between entersyscallblock/exitsyscall
func notetsleepg(n *note, ns int64) bool {
	gp := getg()
	if gp == gp.m.g0 {
		throw("notetsleepg on g0")
	}
	semacreate(gp.m)
	entersyscallblock()
	ok := notetsleep_internal(n, ns, nil, 0)
	exitsyscall()
	return ok
}

func beforeIdle(int64) (*g, bool) {
	return nil, false
}

func checkTimeouts() {}
