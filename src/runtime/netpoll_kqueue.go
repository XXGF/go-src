// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd netbsd openbsd

package runtime

// Integrated network poller (kqueue-based implementation).

import (
	"runtime/internal/atomic"
	"unsafe"
)

var (
	kq int32 = -1

	netpollBreakRd, netpollBreakWr uintptr // for netpollBreak

	netpollWakeSig uint32 // used to avoid duplicate calls of netpollBreak
)

func netpollinit() {
	kq = kqueue()
	if kq < 0 {
		println("runtime: kqueue failed with", -kq)
		throw("runtime: netpollinit failed")
	}
	closeonexec(kq)
	r, w, errno := nonblockingPipe()
	if errno != 0 {
		println("runtime: pipe failed with", -errno)
		throw("runtime: pipe failed")
	}
	ev := keventt{
		filter: _EVFILT_READ,
		flags:  _EV_ADD,
	}
	*(*uintptr)(unsafe.Pointer(&ev.ident)) = uintptr(r)
	n := kevent(kq, &ev, 1, nil, 0, nil)
	if n < 0 {
		println("runtime: kevent failed with", -n)
		throw("runtime: kevent failed")
	}
	netpollBreakRd = uintptr(r)
	netpollBreakWr = uintptr(w)
}

func netpollIsPollDescriptor(fd uintptr) bool {
	return fd == uintptr(kq) || fd == netpollBreakRd || fd == netpollBreakWr
}

func netpollopen(fd uintptr, pd *pollDesc) int32 {
	// Arm both EVFILT_READ and EVFILT_WRITE in edge-triggered mode (EV_CLEAR)
	// for the whole fd lifetime. The notifications are automatically unregistered
	// when fd is closed.
	var ev [2]keventt
	*(*uintptr)(unsafe.Pointer(&ev[0].ident)) = fd
	ev[0].filter = _EVFILT_READ
	ev[0].flags = _EV_ADD | _EV_CLEAR
	ev[0].fflags = 0
	ev[0].data = 0
	ev[0].udata = (*byte)(unsafe.Pointer(pd))
	ev[1] = ev[0]
	ev[1].filter = _EVFILT_WRITE
	n := kevent(kq, &ev[0], 2, nil, 0, nil)
	if n < 0 {
		return -n
	}
	return 0
}

func netpollclose(fd uintptr) int32 {
	// Don't need to unregister because calling close()
	// on fd will remove any kevents that reference the descriptor.
	return 0
}

func netpollarm(pd *pollDesc, mode int) {
	throw("runtime: unused")
}

// netpollBreak interrupts a kevent.
func netpollBreak() {
	if atomic.Cas(&netpollWakeSig, 0, 1) {
		for {
			var b byte
			n := write(netpollBreakWr, unsafe.Pointer(&b), 1)
			if n == 1 || n == -_EAGAIN {
				break
			}
			if n == -_EINTR {
				continue
			}
			println("runtime: netpollBreak write failed with", -n)
			throw("runtime: netpollBreak write failed")
		}
	}
}

/*
	用于检查网络连接的就绪状态，并返回变为可运行状态的 Goroutine 列表。

	netpoll 函数用于检查网络连接的就绪状态。
	返回变为可运行状态的 Goroutine 列表。
	delay 参数的含义：
		delay < 0：无限期阻塞。
		delay == 0：不阻塞，只进行轮询。
		delay > 0：阻塞最多指定的纳秒数。
*/
// netpoll checks for ready network connections.
// Returns list of goroutines that become runnable.
// delay < 0: blocks indefinitely
// delay == 0: does not block, just polls
// delay > 0: block for up to that many nanoseconds
func netpoll(delay int64) gList {
	// 如果 kq（kqueue 文件描述符）为 -1，表示 kqueue 未初始化，直接返回一个空的 Goroutine 列表。
	if kq == -1 {
		return gList{}
	}
	// tp 是一个指向 timespec 结构的指针，用于指定阻塞时间。
	var tp *timespec
	// ts 是一个 timespec 结构，用于存储时间值。
	var ts timespec
	if delay < 0 {
		tp = nil
	} else if delay == 0 {
		tp = &ts
	} else {
		ts.setNsec(delay)
		if ts.tv_sec > 1e6 {
			// Darwin returns EINVAL if the sleep time is too long.
			ts.tv_sec = 1e6
		}
		tp = &ts
	}
	var events [64]keventt
	// retry 标签用于在 kevent 调用被中断时重新尝试。
retry:
	// 调用 kevent 系统调用来检查网络事件。
	// kq：kqueue 文件描述符。
	// nil 和 0：表示不传递任何要监视的事件。
	// &events[0]：指向 events 数组的指针，用于接收就绪的事件。
	// int32(len(events))：events 数组的长度。
	// tp：指向 timespec 结构的指针，用于指定阻塞时间。
	n := kevent(kq, nil, 0, &events[0], int32(len(events)), tp)
	// 如果 kevent 返回值 n 小于 0，表示发生了错误。
	if n < 0 {
		if n != -_EINTR {
			println("runtime: kevent on fd", kq, "failed with", -n)
			throw("runtime: netpoll failed")
		}
		// If a timed sleep was interrupted, just return to
		// recalculate how long we should sleep now.
		if delay > 0 {
			return gList{}
		}
		goto retry
	}
	// 定义一个 gList 类型的变量 toRun，用于存储变为可运行状态的 Goroutine。
	var toRun gList
	for i := 0; i < int(n); i++ {
		ev := &events[i]
		// 如果事件的 ident 字段等于 netpollBreakRd，表示这是一个特殊的中断事件。
		if uintptr(ev.ident) == netpollBreakRd {
			if ev.filter != _EVFILT_READ {
				println("runtime: netpoll: break fd ready for", ev.filter)
				throw("runtime: netpoll: break fd ready for something unexpected")
			}
			// 如果 delay 不等于 0，表示这是一个阻塞的轮询：
			if delay != 0 {
				// netpollBreak could be picked up by a
				// nonblocking poll. Only read the byte
				// if blocking.
				var tmp [16]byte
				// 读取 netpollBreakRd 文件描述符中的数据。
				read(int32(netpollBreakRd), noescape(unsafe.Pointer(&tmp[0])), int32(len(tmp)))
				// 将 netpollWakeSig 原子变量设置为 0。
				atomic.Store(&netpollWakeSig, 0)
			}
			continue
		}

		var mode int32
		// 根据事件的 filter 字段设置 mode 变量
		switch ev.filter {
		case _EVFILT_READ:
			// 如果 filter 是 _EVFILT_READ，表示有数据可读，设置 mode 为 'r'。
			mode += 'r'

			// On some systems when the read end of a pipe
			// is closed the write end will not get a
			// _EVFILT_WRITE event, but will get a
			// _EVFILT_READ event with EV_EOF set.
			// Note that setting 'w' here just means that we
			// will wake up a goroutine waiting to write;
			// that goroutine will try the write again,
			// and the appropriate thing will happen based
			// on what that write returns (success, EPIPE, EAGAIN).
			// 如果事件的 flags 字段包含 _EV_EOF，表示读端关闭，设置 mode 为 'rw'，以唤醒等待写入的 Goroutine。
			if ev.flags&_EV_EOF != 0 {
				mode += 'w'
			}

		case _EVFILT_WRITE:
			// 如果 filter 是 _EVFILT_WRITE，表示可以写入数据，设置 mode 为 'w'。
			mode += 'w'
		}
		// 如果 mode 不为 0，表示有可处理的事件：
		if mode != 0 {
			// 获取事件的用户数据 udata，并将其转换为 pollDesc 指针 pd。
			pd := (*pollDesc)(unsafe.Pointer(ev.udata))
			pd.everr = false
			if ev.flags == _EV_ERROR {
				pd.everr = true
			}
			// 调用 netpollready 函数，unblock 等待读或等待写的 Goroutine。
			netpollready(&toRun, pd, mode)
		}
	}
	// 返回从网络轮询器中获取到的所有可运行G
	return toRun
}
