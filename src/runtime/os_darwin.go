// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

type mOS struct {
	initialized bool
	mutex       pthreadmutex
	cond        pthreadcond
	count       int
}

func unimplemented(name string) {
	println(name, "not implemented")
	*(*int)(unsafe.Pointer(uintptr(1231))) = 1231
}

//go:nosplit
func semacreate(mp *m) {
	if mp.initialized {
		return
	}
	mp.initialized = true
	// 初始化M的信号相关的分布式锁
	if err := pthread_mutex_init(&mp.mutex, nil); err != 0 {
		throw("pthread_mutex_init")
	}
	// 初始化M的信号量cond，用于接受唤醒信号
	if err := pthread_cond_init(&mp.cond, nil); err != 0 {
		throw("pthread_cond_init")
	}
}

//go:nosplit
func semasleep(ns int64) int32 {
	var start int64
	if ns >= 0 {
		start = nanotime()
	}
	mp := getg().m
	pthread_mutex_lock(&mp.mutex)
	for {
		if mp.count > 0 {
			mp.count--
			pthread_mutex_unlock(&mp.mutex)
			return 0
		}
		if ns >= 0 {
			spent := nanotime() - start
			if spent >= ns {
				pthread_mutex_unlock(&mp.mutex)
				return -1
			}
			var t timespec
			t.setNsec(ns - spent)
			err := pthread_cond_timedwait_relative_np(&mp.cond, &mp.mutex, &t)
			if err == _ETIMEDOUT {
				pthread_mutex_unlock(&mp.mutex)
				return -1
			}
		} else {
			pthread_cond_wait(&mp.cond, &mp.mutex)
		}
	}
}

//go:nosplit
func semawakeup(mp *m) {
	pthread_mutex_lock(&mp.mutex)
	mp.count++
	if mp.count > 0 {
		// 调用 pthread_cond_signal 函数，发送一个信号给 mp 结构体中的条件变量 cond。这通常用于唤醒等待该条件变量的线程。
		pthread_cond_signal(&mp.cond)
	}
	pthread_mutex_unlock(&mp.mutex)
}

// The read and write file descriptors used by the sigNote functions.
var sigNoteRead, sigNoteWrite int32

// sigNoteSetup initializes an async-signal-safe note.
//
// The current implementation of notes on Darwin is not async-signal-safe,
// because the functions pthread_mutex_lock, pthread_cond_signal, and
// pthread_mutex_unlock, called by semawakeup, are not async-signal-safe.
// There is only one case where we need to wake up a note from a signal
// handler: the sigsend function. The signal handler code does not require
// all the features of notes: it does not need to do a timed wait.
// This is a separate implementation of notes, based on a pipe, that does
// not support timed waits but is async-signal-safe.
func sigNoteSetup(*note) {
	if sigNoteRead != 0 || sigNoteWrite != 0 {
		throw("duplicate sigNoteSetup")
	}
	var errno int32
	sigNoteRead, sigNoteWrite, errno = pipe()
	if errno != 0 {
		throw("pipe failed")
	}
	closeonexec(sigNoteRead)
	closeonexec(sigNoteWrite)

	// Make the write end of the pipe non-blocking, so that if the pipe
	// buffer is somehow full we will not block in the signal handler.
	// Leave the read end of the pipe blocking so that we will block
	// in sigNoteSleep.
	setNonblock(sigNoteWrite)
}

// sigNoteWakeup wakes up a thread sleeping on a note created by sigNoteSetup.
func sigNoteWakeup(*note) {
	var b byte
	write(uintptr(sigNoteWrite), unsafe.Pointer(&b), 1)
}

// sigNoteSleep waits for a note created by sigNoteSetup to be woken.
func sigNoteSleep(*note) {
	entersyscallblock()
	var b byte
	read(sigNoteRead, unsafe.Pointer(&b), 1)
	exitsyscall()
}

// BSD interface for threading.
func osinit() {
	// pthread_create delayed until end of goenvs so that we
	// can look at the environment first.

	ncpu = getncpu()
	physPageSize = getPageSize()
}

const (
	_CTL_HW      = 6
	_HW_NCPU     = 3
	_HW_PAGESIZE = 7
)

func getncpu() int32 {
	// Use sysctl to fetch hw.ncpu.
	mib := [2]uint32{_CTL_HW, _HW_NCPU}
	out := uint32(0)
	nout := unsafe.Sizeof(out)
	ret := sysctl(&mib[0], 2, (*byte)(unsafe.Pointer(&out)), &nout, nil, 0)
	if ret >= 0 && int32(out) > 0 {
		return int32(out)
	}
	return 1
}

func getPageSize() uintptr {
	// Use sysctl to fetch hw.pagesize.
	mib := [2]uint32{_CTL_HW, _HW_PAGESIZE}
	out := uint32(0)
	nout := unsafe.Sizeof(out)
	ret := sysctl(&mib[0], 2, (*byte)(unsafe.Pointer(&out)), &nout, nil, 0)
	if ret >= 0 && int32(out) > 0 {
		return uintptr(out)
	}
	return 0
}

var urandom_dev = []byte("/dev/urandom\x00")

//go:nosplit
func getRandomData(r []byte) {
	fd := open(&urandom_dev[0], 0 /* O_RDONLY */, 0)
	n := read(fd, unsafe.Pointer(&r[0]), int32(len(r)))
	closefd(fd)
	extendRandom(r, int(n))
}

func goenvs() {
	goenvs_unix()
}

/*
	这段代码是 Go 运行时中用于创建新操作系统线程的函数 newosproc 的实现。
	它使用了 POSIX 线程库（pthread）来创建新线程，并进行了一些必要的初始化和错误处理。
*/
// May run with m.p==nil, so write barriers are not allowed.
//go:nowritebarrierrec
func newosproc(mp *m) {
	// 获取 mp.g0 的栈顶指针。
	stk := unsafe.Pointer(mp.g0.stack.hi)
	// 这段代码永远不会执行，但它包含了一些调试信息，可以在需要时启用。
	if false {
		print("newosproc stk=", stk, " m=", mp, " g=", mp.g0, " id=", mp.id, " ostk=", &mp, "\n")
	}

	// Initialize an attribute object.
	// 声明一个 pthreadattr 变量，用于存储线程属性。
	var attr pthreadattr
	var err int32
	// 初始化线程属性对象。如果失败，写入错误信息并退出程序。
	err = pthread_attr_init(&attr)
	if err != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}

	// Find out OS stack size for our own stack guard.
	// 声明一个变量 stacksize，用于存储栈大小。
	var stacksize uintptr
	// 获取线程栈大小。如果失败，写入错误信息并退出程序。
	if pthread_attr_getstacksize(&attr, &stacksize) != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}
	// 将获取到的栈大小赋值给 mp.g0.stack.hi。
	mp.g0.stack.hi = stacksize // for mstart
	//mSysStatInc(&memstats.stacks_sys, stacksize) //TODO: do this?

	// Tell the pthread library we won't join with this thread.
	// 设置线程为分离状态，这样线程结束时资源会自动释放。如果失败，写入错误信息并退出程序。
	if pthread_attr_setdetachstate(&attr, _PTHREAD_CREATE_DETACHED) != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}

	// Finally, create the thread. It starts at mstart_stub, which does some low-level
	// setup and then calls mstart.
	// NOTE:
	// 新创建的线程将从一个名为 mstart_stub 的函数开始执行。
	// mstart_stub 是用汇编实现，负责执行一些必要的低级操作，以确保线程能够正确运行。
	// 在完成低级设置后，mstart_stub 将调用另一个名为 mstart 的函数。
	// mstart 会调用 schedule，获取G和执行G。

	// NOTE：在创建线程之前和之后，使用 sigprocmask 设置信号掩码，以确保线程创建过程不被信号中断。

	var oset sigset
	// 设置信号掩码，阻止所有信号，并保存旧的信号掩码。
	sigprocmask(_SIG_SETMASK, &sigset_all, &oset)
	// 调用 pthread_create 函数创建一个新线程。
	err = pthread_create(&attr, funcPC(mstart_stub), unsafe.Pointer(mp))
	// 恢复旧的信号掩码。
	sigprocmask(_SIG_SETMASK, &oset, nil)
	if err != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}
}

// glue code to call mstart from pthread_create.
func mstart_stub()

// newosproc0 is a version of newosproc that can be called before the runtime
// is initialized.
//
// This function is not safe to use after initialization as it does not pass an M as fnarg.
//
//go:nosplit
func newosproc0(stacksize uintptr, fn uintptr) {
	// Initialize an attribute object.
	var attr pthreadattr
	var err int32
	err = pthread_attr_init(&attr)
	if err != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}

	// The caller passes in a suggested stack size,
	// from when we allocated the stack and thread ourselves,
	// without libpthread. Now that we're using libpthread,
	// we use the OS default stack size instead of the suggestion.
	// Find out that stack size for our own stack guard.
	if pthread_attr_getstacksize(&attr, &stacksize) != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}
	g0.stack.hi = stacksize // for mstart
	mSysStatInc(&memstats.stacks_sys, stacksize)

	// Tell the pthread library we won't join with this thread.
	if pthread_attr_setdetachstate(&attr, _PTHREAD_CREATE_DETACHED) != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}

	// Finally, create the thread. It starts at mstart_stub, which does some low-level
	// setup and then calls mstart.
	var oset sigset
	sigprocmask(_SIG_SETMASK, &sigset_all, &oset)
	err = pthread_create(&attr, fn, nil)
	sigprocmask(_SIG_SETMASK, &oset, nil)
	if err != 0 {
		write(2, unsafe.Pointer(&failthreadcreate[0]), int32(len(failthreadcreate)))
		exit(1)
	}
}

var failallocatestack = []byte("runtime: failed to allocate stack for the new OS thread\n")
var failthreadcreate = []byte("runtime: failed to create new OS thread\n")

// Called to do synchronous initialization of Go code built with
// -buildmode=c-archive or -buildmode=c-shared.
// None of the Go runtime is initialized.
//go:nosplit
//go:nowritebarrierrec
func libpreinit() {
	initsig(true)
}

/*
	详细解释一下 mp.gsignal 是如何起作用的？
	1. mp.gsignal 是一个指向 goroutine 的指针，专门用于处理信号的 goroutine。每个 m（操作系统线程）都有一个与之关联的信号处理 goroutine。
	2. malg(size int) 是一个用于分配 goroutine 的内存的函数。在这里，它为 mp.gsignal 分配了 32KB 的内存。这是为了确保在处理信号时有足够的栈空间。根据注释，OS X 系统要求的最小栈大小为 8KB，因此这里分配了更大的空间以确保安全。
	3. 这一行代码将 mp（当前的操作系统线程）赋值给 mp.gsignal.m。这意味着 mp.gsignal goroutine 将会知道它所运行的操作系统线程。这对于信号处理是重要的，因为信号处理需要在特定的线程上下文中进行。

	mp.gsignal 的作用
	信号处理: mp.gsignal goroutine 负责处理与该操作系统线程相关的信号。当操作系统接收到信号时，它会将信号传递给相应的 goroutine 进行处理。
	上下文关联: 通过将 mp 赋值给 mp.gsignal.m，信号处理 goroutine 可以访问与其关联的操作系统线程的上下文。这对于在处理信号时执行特定的操作（如恢复状态、调度其他 goroutine 等）是必要的。
	并发安全: Go 的运行时系统设计为在 goroutine 之间安全地处理信号。通过为每个 m 分配一个专门的信号处理 goroutine，Go 可以确保信号处理不会干扰其他 goroutine 的执行。

*/
// Called to initialize a new m (including the bootstrap m).
// Called on the parent thread (main thread in case of bootstrap), can allocate memory.
// 从一个父线程上进行调用（引导时为主线程），可以分配内存
func mpreinit(mp *m) {
	mp.gsignal = malg(32 * 1024) // OS X wants >= 8K
	mp.gsignal.m = mp
}

// Called to initialize a new m (including the bootstrap m).
// Called on the new thread, cannot allocate memory.
func minit() {
	// The alternate signal stack is buggy on arm64.
	// The signal handler handles it directly.
	if GOARCH != "arm64" {
		minitSignalStack()
	}
	minitSignalMask()
	getg().m.procid = uint64(pthread_self())
}

// Called from dropm to undo the effect of an minit.
//go:nosplit
func unminit() {
	// The alternate signal stack is buggy on arm64.
	// See minit.
	if GOARCH != "arm64" {
		unminitSignals()
	}
}

//go:nosplit
func osyield() {
	usleep(1)
}

const (
	_NSIG        = 32
	_SI_USER     = 0 /* empirically true, but not what headers say */
	_SIG_BLOCK   = 1
	_SIG_UNBLOCK = 2
	_SIG_SETMASK = 3
	_SS_DISABLE  = 4
)

//extern SigTabTT runtime·sigtab[];

type sigset uint32

var sigset_all = ^sigset(0)

//go:nosplit
//go:nowritebarrierrec
func setsig(i uint32, fn uintptr) {
	var sa usigactiont
	// 这行代码设置了信号处理的标志：
	//_SA_SIGINFO: 允许信号处理程序接收额外的信息（如信号的来源）。
	//_SA_ONSTACK: 指示信号处理程序在一个新的信号栈上执行。
	//_SA_RESTART: 使某些系统调用在信号处理后自动重启。
	sa.sa_flags = _SA_SIGINFO | _SA_ONSTACK | _SA_RESTART
	// 这行代码将信号屏蔽字设置为全1，表示在处理信号时屏蔽所有信号。
	sa.sa_mask = ^uint32(0)
	if fn == funcPC(sighandler) {
		if iscgo {
			fn = funcPC(cgoSigtramp)
		} else {
			// 将fn设置为 sigtramp 函数的地址
			// sigtramp 是信号处理函数
			fn = funcPC(sigtramp)
		}
	}
	// 这段代码将信号处理程序的地址存储到 sa 结构体中，并调用 sigaction 函数来注册这个信号处理程序。
	// sigaction 是一个系统调用，用于更改信号的处理方式。
	*(*uintptr)(unsafe.Pointer(&sa.__sigaction_u)) = fn
	sigaction(i, &sa, nil)
}

// 汇编实现代码的地址：runtime/sys_linux_386.s 431
/*
	TEXT runtime·sigtramp(SB),NOSPLIT,$28

	......

	// 这里会调到runtime·sigtrampgo
	CALL	runtime·sigtrampgo(SB)
*/
// sigtramp is the callback from libc when a signal is received.
// It is called with the C calling convention.
func sigtramp()
func cgoSigtramp()

//go:nosplit
//go:nowritebarrierrec
func setsigstack(i uint32) {
	var osa usigactiont
	sigaction(i, nil, &osa)
	handler := *(*uintptr)(unsafe.Pointer(&osa.__sigaction_u))
	if osa.sa_flags&_SA_ONSTACK != 0 {
		return
	}
	var sa usigactiont
	*(*uintptr)(unsafe.Pointer(&sa.__sigaction_u)) = handler
	sa.sa_mask = osa.sa_mask
	sa.sa_flags = osa.sa_flags | _SA_ONSTACK
	sigaction(i, &sa, nil)
}

/*
	getsig 函数的作用是获取与指定信号编号 i 相关的信号处理程序的地址。
*/
//go:nosplit
//go:nowritebarrierrec
func getsig(i uint32) uintptr {
	var sa usigactiont
	sigaction(i, nil, &sa)
	return *(*uintptr)(unsafe.Pointer(&sa.__sigaction_u))
}

// setSignaltstackSP sets the ss_sp field of a stackt.
//go:nosplit
func setSignalstackSP(s *stackt, sp uintptr) {
	*(*uintptr)(unsafe.Pointer(&s.ss_sp)) = sp
}

//go:nosplit
//go:nowritebarrierrec
func sigaddset(mask *sigset, i int) {
	*mask |= 1 << (uint32(i) - 1)
}

/*
	sigdelset 函数用于从信号集（sigset）中删除特定的信号。
	1. mask *sigset: 这是一个指向信号集的指针。sigset 是一个类型，通常用于表示一组信号。
	2. i int: 这是要从信号集中删除的信号的索引。信号的索引通常是从 1 开始的，因此在函数内部需要减去 1。
*/
func sigdelset(mask *sigset, i int) {
	// uint32(i) - 1：因为信号索引是从1开始，所以这里减去1，表示在信号集中，索引还是从0开始
	// 1 << (uint32(i) - 1): 将1左移 信号索引的位数，即找到信号在信号集中的bit
	// *mask &^= 1 << (uint32(i) - 1): &^ 操作符被称为 "位清除" 操作符（bit clear operator）。所以这里是清理 1 << (uint32(i) - 1) 位上的数据。

	// 所以这个表达式的意思是，找到信号在信号集中的bit，然后清除这个bit上的数据。
	*mask &^= 1 << (uint32(i) - 1)
}

//go:linkname executablePath os.executablePath
var executablePath string

func sysargs(argc int32, argv **byte) {
	// skip over argv, envv and the first string will be the path
	n := argc + 1
	for argv_index(argv, n) != nil {
		n++
	}
	executablePath = gostringnocopy(argv_index(argv, n+1))

	// strip "executable_path=" prefix if available, it's added after OS X 10.11.
	const prefix = "executable_path="
	if len(executablePath) > len(prefix) && executablePath[:len(prefix)] == prefix {
		executablePath = executablePath[len(prefix):]
	}
}

func signalM(mp *m, sig int) {
	pthread_kill(pthread(mp.procid), uint32(sig))
}
