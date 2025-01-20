// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

/*
	libcCall 是 Go 语言运行时中的一个函数，用于调用 C 库函数（libc 函数）。
	这个函数的实现涉及一些底层的细节，包括切换到系统栈、保存调用点信息以便进行性能分析和回溯等。

	libcCall 是 Go 运行时系统中的一个函数，用于调用底层的 C 库函数。它接受两个参数：一个是 C 函数的地址，另一个是传递给 C 函数的参数。
*/
// Call fn with arg as its argument. Return what fn returns.
// fn is the raw pc value of the entry point of the desired function.
// Switches to the system stack, if not already there.
// Preserves the calling point as the location where a profiler traceback will begin.
//go:nosplit
func libcCall(fn, arg unsafe.Pointer) int32 {
	// Leave caller's PC/SP/G around for traceback.
	// 获取当前的 Goroutine。
	gp := getg()
	// 获取当前 Goroutine 所在的 M（Machine）。
	var mp *m
	if gp != nil {
		mp = gp.m
	}
	// 如果 mp 不为 nil 且 mp.libcallsp 为 0，表示这是第一次调用 libcCall，需要保存调用点信息。
	if mp != nil && mp.libcallsp == 0 {
		// 保存当前 Goroutine。
		mp.libcallg.set(gp)
		//  保存调用者的程序计数器（PC）。
		mp.libcallpc = getcallerpc()
		// sp must be the last, because once async cpu profiler finds
		// all three values to be non-zero, it will use them
		// 保存调用者的栈指针（SP）。
		mp.libcallsp = getcallersp()
	} else {
		// Make sure we don't reset libcallsp. This makes
		// libcCall reentrant; We remember the g/pc/sp for the
		// first call on an M, until that libcCall instance
		// returns.  Reentrance only matters for signals, as
		// libc never calls back into Go.  The tricky case is
		// where we call libcX from an M and record g/pc/sp.
		// Before that call returns, a signal arrives on the
		// same M and the signal handling code calls another
		// libc function.  We don't want that second libcCall
		// from within the handler to be recorded, and we
		// don't want that call's completion to zero
		// libcallsp.
		// We don't need to set libcall* while we're in a sighandler
		// (even if we're not currently in libc) because we block all
		// signals while we're handling a signal. That includes the
		// profile signal, which is the one that uses the libcall* info.
		mp = nil
	}
	// 调用底层的汇编函数 asmcgocall，实际执行 C 函数调用。fn就是要调用的C函数
	res := asmcgocall(fn, arg)
	// 如果 mp 不为 nil，在调用完成后清除 mp.libcallsp，表示调用结束。
	// asmcgocall 函数负责切换到系统栈并调用指定的 C 函数。
	if mp != nil {
		mp.libcallsp = 0
	}
	return res
}

// The X versions of syscall expect the libc call to return a 64-bit result.
// Otherwise (the non-X version) expects a 32-bit result.
// This distinction is required because an error is indicated by returning -1,
// and we need to know whether to check 32 or 64 bits of the result.
// (Some libc functions that return 32 bits put junk in the upper 32 bits of AX.)

//go:linkname syscall_syscall syscall.syscall
//go:nosplit
//go:cgo_unsafe_args
func syscall_syscall(fn, a1, a2, a3 uintptr) (r1, r2, err uintptr) {
	entersyscall()
	libcCall(unsafe.Pointer(funcPC(syscall)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscall()

//go:linkname syscall_syscallX syscall.syscallX
//go:nosplit
//go:cgo_unsafe_args
func syscall_syscallX(fn, a1, a2, a3 uintptr) (r1, r2, err uintptr) {
	entersyscallblock()
	libcCall(unsafe.Pointer(funcPC(syscallX)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscallX()

//go:linkname syscall_syscall6 syscall.syscall6
//go:nosplit
//go:cgo_unsafe_args
func syscall_syscall6(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, err uintptr) {
	entersyscall()
	libcCall(unsafe.Pointer(funcPC(syscall6)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscall6()

//go:linkname syscall_syscall6X syscall.syscall6X
//go:nosplit
//go:cgo_unsafe_args
func syscall_syscall6X(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, err uintptr) {
	entersyscall()
	libcCall(unsafe.Pointer(funcPC(syscall6X)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscall6X()

//go:linkname syscall_syscallPtr syscall.syscallPtr
//go:nosplit
//go:cgo_unsafe_args
func syscall_syscallPtr(fn, a1, a2, a3 uintptr) (r1, r2, err uintptr) {
	entersyscall()
	libcCall(unsafe.Pointer(funcPC(syscallPtr)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscallPtr()

//go:linkname syscall_rawSyscall syscall.rawSyscall
//go:nosplit
//go:cgo_unsafe_args
func syscall_rawSyscall(fn, a1, a2, a3 uintptr) (r1, r2, err uintptr) {
	libcCall(unsafe.Pointer(funcPC(syscall)), unsafe.Pointer(&fn))
	return
}

//go:linkname syscall_rawSyscall6 syscall.rawSyscall6
//go:nosplit
//go:cgo_unsafe_args
func syscall_rawSyscall6(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1, r2, err uintptr) {
	libcCall(unsafe.Pointer(funcPC(syscall6)), unsafe.Pointer(&fn))
	return
}

// syscallNoErr is used in crypto/x509 to call into Security.framework and CF.

//go:linkname crypto_x509_syscall crypto/x509/internal/macos.syscall
//go:nosplit
//go:cgo_unsafe_args
func crypto_x509_syscall(fn, a1, a2, a3, a4, a5, a6 uintptr) (r1 uintptr) {
	entersyscall()
	libcCall(unsafe.Pointer(funcPC(syscallNoErr)), unsafe.Pointer(&fn))
	exitsyscall()
	return
}
func syscallNoErr()

// The *_trampoline functions convert from the Go calling convention to the C calling convention
// and then call the underlying libc function.  They are defined in sys_darwin_$ARCH.s.

//go:nosplit
//go:cgo_unsafe_args
func pthread_attr_init(attr *pthreadattr) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_attr_init_trampoline)), unsafe.Pointer(&attr))
}
func pthread_attr_init_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_attr_getstacksize(attr *pthreadattr, size *uintptr) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_attr_getstacksize_trampoline)), unsafe.Pointer(&attr))
}
func pthread_attr_getstacksize_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_attr_setdetachstate(attr *pthreadattr, state int) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_attr_setdetachstate_trampoline)), unsafe.Pointer(&attr))
}
func pthread_attr_setdetachstate_trampoline()

/*
	这段代码定义了一个 pthread_create 函数，用于创建一个新的线程。它使用了 Go 的 libcCall 函数来调用底层的 C 库函数 pthread_create。
	此外，还定义了一个 pthread_create_trampoline 函数，作为调用 pthread_create 的中间跳板。


*/
//go:nosplit
//go:cgo_unsafe_args
func pthread_create(attr *pthreadattr, start uintptr, arg unsafe.Pointer) int32 {
	// 使用 libcCall 函数调用底层的 C 库函数 pthread_create。
	// funcPC(pthread_create_trampoline)：获取 pthread_create_trampoline 函数的程序计数器（PC）。
	// unsafe.Pointer(&attr)：将 attr 的地址转换为 unsafe.Pointer 类型，并传递给 libcCall。
	return libcCall(unsafe.Pointer(funcPC(pthread_create_trampoline)), unsafe.Pointer(&attr))
}
func pthread_create_trampoline()

// func pthread_create_trampoline()：声明一个名为 pthread_create_trampoline 的空函数。
// 这个函数的具体实现通常在汇编代码中定义，用于将 Go 函数调用转换为 C 函数调用。
// 汇编实现如下：
/*
TEXT runtime·pthread_create_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	0(DI), SI	// arg 2 attr
	MOVQ	8(DI), DX	// arg 3 start
	MOVQ	16(DI), CX	// arg 4 arg
	MOVQ	SP, DI		// arg 1 &threadid (which we throw away)
	CALL	libc_pthread_create(SB)
	MOVQ	BP, SP
	POPQ	BP
	RET
*/
/*
	//go:cgo_import_dynamic libc_pthread_create pthread_create "/usr/lib/libSystem.B.dylib"

	//go:cgo_import_dynamic 是一个编译指示（directive），用于告诉 Go 编译器如何处理动态链接库中的符号。
	具体来说，它用于导入动态链接库中的符号，以便在 Go 代码中使用这些符号。

	1. //go:cgo_import_dynamic: 这是一个编译指示，告诉 Go 编译器导入一个动态链接库中的符号。cgo 是 Go 中用于与 C 语言进行互操作的工具。
	2. libc_pthread_create: 这是在 Go 代码中使用的符号名称。通过这个名称，Go 代码可以引用动态链接库中的 pthread_create 函数。
	3. pthread_create: 这是动态链接库中的实际符号名称。在这个例子中，它是标准 C 库中的 pthread_create 函数。
	4. "/usr/lib/libSystem.B.dylib": 这是动态链接库的路径。在 macOS 系统上，libSystem.B.dylib 是包含标准 C 库函数的动态链接库。

	背景：
	在 macOS 系统上，libSystem.B.dylib 是一个核心动态链接库，包含了许多基础的系统函数，包括线程管理、文件操作、内存管理等。pthread_create 是其中的一个函数，用于创建新的线程。

	使用场景：
	在 Go 运行时中，有时需要直接调用操作系统提供的底层函数，例如线程创建函数 pthread_create。为了实现这一点，Go 运行时需要导入这些函数的符号，以便在 Go 代码中调用它们。
*/

//go:nosplit
//go:cgo_unsafe_args
func raise(sig uint32) {
	libcCall(unsafe.Pointer(funcPC(raise_trampoline)), unsafe.Pointer(&sig))
}
func raise_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_self() (t pthread) {
	libcCall(unsafe.Pointer(funcPC(pthread_self_trampoline)), unsafe.Pointer(&t))
	return
}
func pthread_self_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_kill(t pthread, sig uint32) {
	libcCall(unsafe.Pointer(funcPC(pthread_kill_trampoline)), unsafe.Pointer(&t))
	return
}
func pthread_kill_trampoline()

// mmap is used to do low-level memory allocation via mmap. Don't allow stack
// splits, since this function (used by sysAlloc) is called in a lot of low-level
// parts of the runtime and callers often assume it won't acquire any locks.
// go:nosplit
func mmap(addr unsafe.Pointer, n uintptr, prot, flags, fd int32, off uint32) (unsafe.Pointer, int) {
	args := struct {
		addr            unsafe.Pointer
		n               uintptr
		prot, flags, fd int32
		off             uint32
		ret1            unsafe.Pointer
		ret2            int
	}{addr, n, prot, flags, fd, off, nil, 0}
	libcCall(unsafe.Pointer(funcPC(mmap_trampoline)), unsafe.Pointer(&args))
	return args.ret1, args.ret2
}
func mmap_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func munmap(addr unsafe.Pointer, n uintptr) {
	libcCall(unsafe.Pointer(funcPC(munmap_trampoline)), unsafe.Pointer(&addr))
}
func munmap_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func madvise(addr unsafe.Pointer, n uintptr, flags int32) {
	libcCall(unsafe.Pointer(funcPC(madvise_trampoline)), unsafe.Pointer(&addr))
}
func madvise_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func read(fd int32, p unsafe.Pointer, n int32) int32 {
	return libcCall(unsafe.Pointer(funcPC(read_trampoline)), unsafe.Pointer(&fd))
}
func read_trampoline()

func pipe() (r, w int32, errno int32) {
	var p [2]int32
	errno = libcCall(unsafe.Pointer(funcPC(pipe_trampoline)), noescape(unsafe.Pointer(&p)))
	return p[0], p[1], errno
}
func pipe_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func closefd(fd int32) int32 {
	return libcCall(unsafe.Pointer(funcPC(close_trampoline)), unsafe.Pointer(&fd))
}
func close_trampoline()

//go:nosplit
//go:cgo_unsafe_args
//
// This is exported via linkname to assembly in runtime/cgo.
//go:linkname exit
func exit(code int32) {
	libcCall(unsafe.Pointer(funcPC(exit_trampoline)), unsafe.Pointer(&code))
}
func exit_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func usleep(usec uint32) {
	libcCall(unsafe.Pointer(funcPC(usleep_trampoline)), unsafe.Pointer(&usec))
}
func usleep_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func write1(fd uintptr, p unsafe.Pointer, n int32) int32 {
	return libcCall(unsafe.Pointer(funcPC(write_trampoline)), unsafe.Pointer(&fd))
}
func write_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func open(name *byte, mode, perm int32) (ret int32) {
	return libcCall(unsafe.Pointer(funcPC(open_trampoline)), unsafe.Pointer(&name))
}
func open_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func nanotime1() int64 {
	var r struct {
		t            int64  // raw timer
		numer, denom uint32 // conversion factors. nanoseconds = t * numer / denom.
	}
	libcCall(unsafe.Pointer(funcPC(nanotime_trampoline)), unsafe.Pointer(&r))
	// Note: Apple seems unconcerned about overflow here. See
	// https://developer.apple.com/library/content/qa/qa1398/_index.html
	// Note also, numer == denom == 1 is common.
	t := r.t
	if r.numer != 1 {
		t *= int64(r.numer)
	}
	if r.denom != 1 {
		t /= int64(r.denom)
	}
	return t
}
func nanotime_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func walltime1() (int64, int32) {
	var t timeval
	libcCall(unsafe.Pointer(funcPC(walltime_trampoline)), unsafe.Pointer(&t))
	return int64(t.tv_sec), 1000 * t.tv_usec
}
func walltime_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func sigaction(sig uint32, new *usigactiont, old *usigactiont) {
	libcCall(unsafe.Pointer(funcPC(sigaction_trampoline)), unsafe.Pointer(&sig))
}
func sigaction_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func sigprocmask(how uint32, new *sigset, old *sigset) {
	libcCall(unsafe.Pointer(funcPC(sigprocmask_trampoline)), unsafe.Pointer(&how))
}
func sigprocmask_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func sigaltstack(new *stackt, old *stackt) {
	if new != nil && new.ss_flags&_SS_DISABLE != 0 && new.ss_size == 0 {
		// Despite the fact that Darwin's sigaltstack man page says it ignores the size
		// when SS_DISABLE is set, it doesn't. sigaltstack returns ENOMEM
		// if we don't give it a reasonable size.
		// ref: http://lists.llvm.org/pipermail/llvm-commits/Week-of-Mon-20140421/214296.html
		new.ss_size = 32768
	}
	libcCall(unsafe.Pointer(funcPC(sigaltstack_trampoline)), unsafe.Pointer(&new))
}
func sigaltstack_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func raiseproc(sig uint32) {
	libcCall(unsafe.Pointer(funcPC(raiseproc_trampoline)), unsafe.Pointer(&sig))
}
func raiseproc_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func setitimer(mode int32, new, old *itimerval) {
	libcCall(unsafe.Pointer(funcPC(setitimer_trampoline)), unsafe.Pointer(&mode))
}
func setitimer_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func sysctl(mib *uint32, miblen uint32, out *byte, size *uintptr, dst *byte, ndst uintptr) int32 {
	return libcCall(unsafe.Pointer(funcPC(sysctl_trampoline)), unsafe.Pointer(&mib))
}
func sysctl_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func fcntl(fd, cmd, arg int32) int32 {
	return libcCall(unsafe.Pointer(funcPC(fcntl_trampoline)), unsafe.Pointer(&fd))
}
func fcntl_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func kqueue() int32 {
	v := libcCall(unsafe.Pointer(funcPC(kqueue_trampoline)), nil)
	return v
}
func kqueue_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func kevent(kq int32, ch *keventt, nch int32, ev *keventt, nev int32, ts *timespec) int32 {
	return libcCall(unsafe.Pointer(funcPC(kevent_trampoline)), unsafe.Pointer(&kq))
}
func kevent_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_mutex_init(m *pthreadmutex, attr *pthreadmutexattr) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_mutex_init_trampoline)), unsafe.Pointer(&m))
}
func pthread_mutex_init_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_mutex_lock(m *pthreadmutex) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_mutex_lock_trampoline)), unsafe.Pointer(&m))
}
func pthread_mutex_lock_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_mutex_unlock(m *pthreadmutex) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_mutex_unlock_trampoline)), unsafe.Pointer(&m))
}
func pthread_mutex_unlock_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_cond_init(c *pthreadcond, attr *pthreadcondattr) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_cond_init_trampoline)), unsafe.Pointer(&c))
}
func pthread_cond_init_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_cond_wait(c *pthreadcond, m *pthreadmutex) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_cond_wait_trampoline)), unsafe.Pointer(&c))
}
func pthread_cond_wait_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_cond_timedwait_relative_np(c *pthreadcond, m *pthreadmutex, t *timespec) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_cond_timedwait_relative_np_trampoline)), unsafe.Pointer(&c))
}
func pthread_cond_timedwait_relative_np_trampoline()

//go:nosplit
//go:cgo_unsafe_args
func pthread_cond_signal(c *pthreadcond) int32 {
	return libcCall(unsafe.Pointer(funcPC(pthread_cond_signal_trampoline)), unsafe.Pointer(&c))
}
func pthread_cond_signal_trampoline()

// Not used on Darwin, but must be defined.
func exitThread(wait *uint32) {
}

//go:nosplit
func closeonexec(fd int32) {
	fcntl(fd, _F_SETFD, _FD_CLOEXEC)
}

//go:nosplit
func setNonblock(fd int32) {
	flags := fcntl(fd, _F_GETFL, 0)
	fcntl(fd, _F_SETFL, flags|_O_NONBLOCK)
}

// Tell the linker that the libc_* functions are to be found
// in a system library, with the libc_ prefix missing.

/*
	//go:cgo_import_dynamic libc_pthread_create pthread_create "/usr/lib/libSystem.B.dylib"

	//go:cgo_import_dynamic 是一个编译指示（directive），用于告诉 Go 编译器如何处理动态链接库中的符号。
	具体来说，它用于导入动态链接库中的符号，以便在 Go 代码中使用这些符号。

	1. //go:cgo_import_dynamic: 这是一个编译指示，告诉 Go 编译器导入一个动态链接库中的符号。cgo 是 Go 中用于与 C 语言进行互操作的工具。
	2. libc_pthread_create: 这是在 Go 代码中使用的符号名称。通过这个名称，Go 代码可以引用动态链接库中的 pthread_create 函数。
	3. pthread_create: 这是动态链接库中的实际符号名称。在这个例子中，它是标准 C 库中的 pthread_create 函数。
	4. "/usr/lib/libSystem.B.dylib": 这是动态链接库的路径。在 macOS 系统上，libSystem.B.dylib 是包含标准 C 库函数的动态链接库。

	背景：
	在 macOS 系统上，libSystem.B.dylib 是一个核心动态链接库，包含了许多基础的系统函数，包括线程管理、文件操作、内存管理等。pthread_create 是其中的一个函数，用于创建新的线程。

	使用场景：
	在 Go 运行时中，有时需要直接调用操作系统提供的底层函数，例如线程创建函数 pthread_create。为了实现这一点，Go 运行时需要导入这些函数的符号，以便在 Go 代码中调用它们。
*/

//go:cgo_import_dynamic libc_pthread_attr_init pthread_attr_init "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_attr_getstacksize pthread_attr_getstacksize "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_attr_setdetachstate pthread_attr_setdetachstate "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_create pthread_create "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_self pthread_self "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_kill pthread_kill "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_exit exit "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_raise raise "/usr/lib/libSystem.B.dylib"

//go:cgo_import_dynamic libc_open open "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_close close "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_read read "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_write write "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pipe pipe "/usr/lib/libSystem.B.dylib"

//go:cgo_import_dynamic libc_mmap mmap "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_munmap munmap "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_madvise madvise "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_error __error "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_usleep usleep "/usr/lib/libSystem.B.dylib"

//go:cgo_import_dynamic libc_mach_timebase_info mach_timebase_info "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_mach_absolute_time mach_absolute_time "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_gettimeofday gettimeofday "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_sigaction sigaction "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_sigmask pthread_sigmask "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_sigaltstack sigaltstack "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_getpid getpid "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_kill kill "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_setitimer setitimer "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_sysctl sysctl "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_fcntl fcntl "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_kqueue kqueue "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_kevent kevent "/usr/lib/libSystem.B.dylib"

//go:cgo_import_dynamic libc_pthread_mutex_init pthread_mutex_init "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_mutex_lock pthread_mutex_lock "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_mutex_unlock pthread_mutex_unlock "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_cond_init pthread_cond_init "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_cond_wait pthread_cond_wait "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_cond_timedwait_relative_np pthread_cond_timedwait_relative_np "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_pthread_cond_signal pthread_cond_signal "/usr/lib/libSystem.B.dylib"

// Magic incantation to get libSystem and friends actually dynamically linked.
// TODO: Why does the code require this?  See cmd/link/internal/ld/go.go
//go:cgo_import_dynamic _ _ "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic _ _ "/System/Library/Frameworks/Security.framework/Versions/A/Security"
//go:cgo_import_dynamic _ _ "/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation"
