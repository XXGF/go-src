// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

// Should be a built-in for unsafe.Pointer?
//go:nosplit
func add(p unsafe.Pointer, x uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + x)
}

/*
getg函数的作用：
	getg 函数的主要作用是返回当前正在执行的 Goroutine 的指针。
	这个指针指向一个 g 结构体，g 结构体包含了当前 Goroutine 的所有状态信息，包括栈指针、程序计数器、调度器状态等。

getg函数的实现：
	在实际的 Go 运行时代码中，getg 函数的实现依赖于特定的架构和平台。
	所以getg函数是汇编实现。
*/

// 在 Go 运行时中，getg 函数用于获取当前 Goroutine 的指针。
// 编译器会将对这个函数的调用重写为直接从线程本地存储（TLS）或专用寄存器中获取 Goroutine 指针的指令。
// 这种优化可以提高性能，因为直接访问寄存器或 TLS 比调用函数要快得多。
// getg returns the pointer to the current g.
// The compiler rewrites calls to this function into instructions
// that fetch the g directly (from TLS or from the dedicated register).
func getg() *g

/*
	mcall 是 Go 语言运行时中的一个内部函数，用于在 goroutine 和 g0 栈之间切换，并调用指定的函数 fn。

	栈切换：
	1. mcall 函数从当前 goroutine 的栈切换到 g0 栈，并调用传入的函数 fn。
	2. g0 栈是 Go 运行时的调度栈，用于执行调度相关的操作。

	保存上下文：
	1. mcall 会保存当前 goroutine 的程序计数器（PC）和栈指针（SP）到 g->sched 中，以便稍后恢复。
	2. 这允许 mcall 在稍后重新调度时返回到原始的 goroutine。

	函数 fn 的职责：
	1. 传入的函数 fn 负责安排稍后的执行，通常是通过将 goroutine g 记录在某个数据结构中，并导致某个操作稍后调用 ready(g)。
	2. fn 不应该返回，通常会通过调用 schedule 来结束，以便让当前的 M（操作系统线程）运行其他 goroutine。

	调用限制：
	mcall 只能从 goroutine 的栈中调用，不能从 g0 栈或 gsignal 栈中调用。

	逃逸分析：
	这个函数不能标记为 go:noescape，因为如果 fn 是一个栈分配的闭包，并且 fn 将 g 放入运行队列，而 g 在 fn 返回之前执行，那么闭包在执行时将被无效化。
*/
/*
	汇编源码分析：
	1. 保存当前 g 的现场环境 g.sched 中寄存器们的值，有些场景还需要唤醒 g （比如 channel）；
	2. 校验 g !=g0；
	3. 切换 tls 的 g 为 g0；
	切换 g0 的栈（由于每次调用 mcall 函数切换到 g0 栈时，都是切换到 g0.sched.sp 所指的固定位置，因此 g0 栈内存是覆盖重复使用的，不会因为函数不返回问题导致爆栈）；
	调用 goexit0 函数。
*/
// mcall switches from the g to the g0 stack and invokes fn(g),
// where g is the goroutine that made the call.
// mcall saves g's current PC/SP in g->sched so that it can be restored later.
// It is up to fn to arrange for that later execution, typically by recording
// g in a data structure, causing something to call ready(g) later.
// mcall returns to the original goroutine g later, when g has been rescheduled.
// fn must not return at all; typically it ends by calling schedule, to let the m
// run other goroutines.
//
// mcall can only be called from g stacks (not g0, not gsignal).
//
// This must NOT be go:noescape: if fn is a stack-allocated closure,
// fn puts g on a run queue, and g executes before fn returns, the
// closure will be invalidated while it is still executing.
func mcall(fn func(*g))

/*
	systemstack 函数用于在系统栈上运行指定的函数 fn。

	如果 systemstack 是从每个操作系统线程的栈（g0 栈）或信号处理栈（gsignal 栈）调用的，systemstack 会直接调用 fn 并返回。
	否则，systemstack 是从普通 Goroutine 的有限栈调用的。在这种情况下，systemstack 会切换到每个操作系统线程的栈，调用 fn，然后再切换回来。
	通常使用函数字面量作为参数，以便与调用 systemstack 的代码共享输入和输出。
*/
// systemstack runs fn on a system stack.
// If systemstack is called from the per-OS-thread (g0) stack, or
// if systemstack is called from the signal handling (gsignal) stack,
// systemstack calls fn directly and returns.
// Otherwise, systemstack is being called from the limited stack
// of an ordinary goroutine. In this case, systemstack switches
// to the per-OS-thread stack, calls fn, and switches back.
// It is common to use a func literal as the argument, in order
// to share inputs and outputs with the code around the call
// to system stack:
//
//	... set up y ...
//	systemstack(func() {
//		x = bigcall(y)
//	})
//	... use x ...
//
//go:noescape
func systemstack(fn func())

var badsystemstackMsg = "fatal: systemstack called from unexpected goroutine"

//go:nosplit
//go:nowritebarrierrec
func badsystemstack() {
	sp := stringStructOf(&badsystemstackMsg)
	write(2, sp.str, int32(sp.len))
}

// memclrNoHeapPointers clears n bytes starting at ptr.
//
// Usually you should use typedmemclr. memclrNoHeapPointers should be
// used only when the caller knows that *ptr contains no heap pointers
// because either:
//
// *ptr is initialized memory and its type is pointer-free, or
//
// *ptr is uninitialized memory (e.g., memory that's being reused
// for a new allocation) and hence contains only "junk".
//
// 这个方法是用来清除内存的数据的
// The (CPU-specific) implementations of this function are in memclr_*.s.

// memclrNoHeapPointers 清除从 ptr 开始的 n 个字节
// 通常情况下你应该使用 typedmemclr，而 memclrNoHeapPointers 应该仅在调用方知道 *ptr 不包含堆指针的情况下使用
// 因为 *ptr 只能是下面两种情况：
// 1. *ptr 是初始化过的内存，且其类型不是指针。
// 2. *ptr 是未初始化的内存（例如刚被新分配时使用的内存），则只包含 "junk" 垃圾内存
// 见 memclr_*.s 清理过程是汇编实现的，就是一些内存的归零工作
//go:noescape
func memclrNoHeapPointers(ptr unsafe.Pointer, n uintptr)

//go:linkname reflect_memclrNoHeapPointers reflect.memclrNoHeapPointers
func reflect_memclrNoHeapPointers(ptr unsafe.Pointer, n uintptr) {
	memclrNoHeapPointers(ptr, n)
}

// memmove copies n bytes from "from" to "to".
//
// memmove ensures that any pointer in "from" is written to "to" with
// an indivisible write, so that racy reads cannot observe a
// half-written pointer. This is necessary to prevent the garbage
// collector from observing invalid pointers, and differs from memmove
// in unmanaged languages. However, memmove is only required to do
// this if "from" and "to" may contain pointers, which can only be the
// case if "from", "to", and "n" are all be word-aligned.
//
// Implementations are in memmove_*.s.
//
//go:noescape
func memmove(to, from unsafe.Pointer, n uintptr)

//go:linkname reflect_memmove reflect.memmove
func reflect_memmove(to, from unsafe.Pointer, n uintptr) {
	memmove(to, from, n)
}

// exported value for testing
var hashLoad = float32(loadFactorNum) / float32(loadFactorDen)

//go:nosplit
func fastrand() uint32 {
	mp := getg().m
	// Implement xorshift64+: 2 32-bit xorshift sequences added together.
	// Shift triplet [17,7,16] was calculated as indicated in Marsaglia's
	// Xorshift paper: https://www.jstatsoft.org/article/view/v008i14/xorshift.pdf
	// This generator passes the SmallCrush suite, part of TestU01 framework:
	// http://simul.iro.umontreal.ca/testu01/tu01.html
	s1, s0 := mp.fastrand[0], mp.fastrand[1]
	s1 ^= s1 << 17
	s1 = s1 ^ s0 ^ s1>>7 ^ s0>>16
	mp.fastrand[0], mp.fastrand[1] = s0, s1
	return s0 + s1
}

//go:nosplit
func fastrandn(n uint32) uint32 {
	// This is similar to fastrand() % n, but faster.
	// See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint32(uint64(fastrand()) * uint64(n) >> 32)
}

//go:linkname sync_fastrand sync.fastrand
func sync_fastrand() uint32 { return fastrand() }

// in internal/bytealg/equal_*.s
//go:noescape
func memequal(a, b unsafe.Pointer, size uintptr) bool

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

func cgocallback(fn, frame unsafe.Pointer, framesize, ctxt uintptr)

/*
	在 Go 语言中，gogo 是一个低级别的运行时函数，通常用汇编语言实现。
	它的主要作用是切换到指定的 goroutine 并开始执行该 goroutine 的代码。
	gogo 函数会保存当前 goroutine 的上下文（如寄存器状态、程序计数器等），并恢复目标 goroutine 的上下文，从而实现从一个 goroutine 切换到另一个 goroutine。

	gogo 函数的作用
		1. 保存当前上下文：保存当前正在运行的 goroutine 的寄存器状态、程序计数器等上下文信息，以便以后可以恢复。
		2. 恢复目标上下文：恢复目标 goroutine 的寄存器状态、程序计数器等上下文信息，使其能够继续执行。
		3. 切换执行：通过切换上下文，CPU 开始执行目标 goroutine 的代码。

	gogo 函数做了什么：
		1. g0 调用 gogo() 函数时，首先将线程 tls 的 g0 替换为了 g；
		2. 然后通过设置 CPU 的栈顶寄存器 SP 为 g.sched.sp，实现了从 g0 栈到 g 栈的切换；
			保存了其他 gobuf 内的寄存器到 CPU 对应的寄存器，为后续调用 g 做准备；
		3.最后从 g 中取出 g.sched.pc 的值，并通过 JMP 指令从 runtime 代码直接跳转到用户代码执行，完成了 CPU 执行权的转让。

		还记得 g.sched.pc 指向的了啥不？
		g.sched.pc 指向了 go 关键字后边的函数的 fn.fn 指针，也就是执行的第一条指令，cpu 从这里开始运行起来了用户程序代码

		当用户代码正常运行结束，又会发生什么呢？
		用户代码执行完，自然会调用 RET 指令，回到 return address 处继续执行。
		那 return address 又指向的了哪里呢？ return address 指向了 CALL runtime·goexit1(SB) 。因此正常结束的 G 会从这里继续开始执行 goexit1 函数。
*/
func gogo(buf *gobuf)
func gosave(buf *gobuf)

//go:noescape
func jmpdefer(fv *funcval, argp uintptr)
func asminit()
func setg(gg *g)
func breakpoint()

// reflectcall calls fn with a copy of the n argument bytes pointed at by arg.
// After fn returns, reflectcall copies n-retoffset result bytes
// back into arg+retoffset before returning. If copying result bytes back,
// the caller should pass the argument frame type as argtype, so that
// call can execute appropriate write barriers during the copy.
// Package reflect passes a frame type. In package runtime, there is only
// one call that copies results back, in cgocallbackg1, and it does NOT pass a
// frame type, meaning there are no write barriers invoked. See that call
// site for justification.
//
// Package reflect accesses this symbol through a linkname.
func reflectcall(argtype *_type, fn, arg unsafe.Pointer, argsize uint32, retoffset uint32)

func procyield(cycles uint32)

type neverCallThisFunction struct{}

// goexit is the return stub at the top of every goroutine call stack.
// Each goroutine stack is constructed as if goexit called the
// goroutine's entry point function, so that when the entry point
// function returns, it will return to goexit, which will call goexit1
// to perform the actual exit.
//
// This function must never be called directly. Call goexit1 instead.
// gentraceback assumes that goexit terminates the stack. A direct
// call on the stack will cause gentraceback to stop walking the stack
// prematurely and if there is leftover state it may panic.
func goexit(neverCallThisFunction)

// Not all cgocallback_gofunc frames are actually cgocallback_gofunc,
// so not all have these arguments. Mark them uintptr so that the GC
// does not misinterpret memory when the arguments are not present.
// cgocallback_gofunc is not called from go, only from cgocallback,
// so the arguments will be found via cgocallback's pointer-declared arguments.
// See the assembly implementations for more details.
func cgocallback_gofunc(fv, frame, framesize, ctxt uintptr)

// publicationBarrier performs a store/store barrier (a "publication"
// or "export" barrier). Some form of synchronization is required
// between initializing an object and making that object accessible to
// another processor. Without synchronization, the initialization
// writes and the "publication" write may be reordered, allowing the
// other processor to follow the pointer and observe an uninitialized
// object. In general, higher-level synchronization should be used,
// such as locking or an atomic pointer write. publicationBarrier is
// for when those aren't an option, such as in the implementation of
// the memory manager.
//
// There's no corresponding barrier for the read side because the read
// side naturally has a data dependency order. All architectures that
// Go supports or seems likely to ever support automatically enforce
// data dependency ordering.
func publicationBarrier()

// getcallerpc returns the program counter (PC) of its caller's caller.
// getcallersp returns the stack pointer (SP) of its caller's caller.
// The implementation may be a compiler intrinsic; there is not
// necessarily code implementing this on every platform.
//
// For example:
//
//	func f(arg1, arg2, arg3 int) {
//		pc := getcallerpc()
//		sp := getcallersp()
//	}
//
// These two lines find the PC and SP immediately following
// the call to f (where f will return).
//
// The call to getcallerpc and getcallersp must be done in the
// frame being asked about.
//
// The result of getcallersp is correct at the time of the return,
// but it may be invalidated by any subsequent call to a function
// that might relocate the stack in order to grow or shrink it.
// A general rule is that the result of getcallersp should be used
// immediately and can only be passed to nosplit functions.

//go:noescape
func getcallerpc() uintptr

//go:noescape
func getcallersp() uintptr // implemented as an intrinsic on all platforms

// getclosureptr returns the pointer to the current closure.
// getclosureptr can only be used in an assignment statement
// at the entry of a function. Moreover, go:nosplit directive
// must be specified at the declaration of caller function,
// so that the function prolog does not clobber the closure register.
// for example:
//
//	//go:nosplit
//	func f(arg1, arg2, arg3 int) {
//		dx := getclosureptr()
//	}
//
// The compiler rewrites calls to this function into instructions that fetch the
// pointer from a well-known register (DX on x86 architecture, etc.) directly.
func getclosureptr() uintptr

/*
	asmcgocall 函数是 Go 运行时中的一个底层函数，它的主要作用是切换到系统栈并调用指定的 C 函数。
	由于它涉及底层的栈操作和系统调用，用汇编语言实现。
	其实现代码在 asm_amd64.s 等汇编文件中
*/
//go:noescape
func asmcgocall(fn, arg unsafe.Pointer) int32

func morestack()
func morestack_noctxt()
func rt0_go()

// return0 is a stub used to return 0 from deferproc.
// It is called at the very end of deferproc to signal
// the calling Go function that it should not jump
// to deferreturn.
// in asm_*.s
func return0()

// in asm_*.s
// not called directly; definitions here supply type information for traceback.
func call32(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call64(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call128(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call256(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call512(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call1024(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call2048(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call4096(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call8192(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call16384(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call32768(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call65536(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call131072(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call262144(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call524288(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call1048576(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call2097152(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call4194304(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call8388608(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call16777216(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call33554432(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call67108864(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call134217728(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call268435456(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call536870912(typ, fn, arg unsafe.Pointer, n, retoffset uint32)
func call1073741824(typ, fn, arg unsafe.Pointer, n, retoffset uint32)

func systemstack_switch()

// alignUp rounds n up to a multiple of a. a must be a power of 2.
func alignUp(n, a uintptr) uintptr {
	return (n + a - 1) &^ (a - 1)
}

// alignDown rounds n down to a multiple of a. a must be a power of 2.
func alignDown(n, a uintptr) uintptr {
	return n &^ (a - 1)
}

// divRoundUp returns ceil(n / a).
func divRoundUp(n, a uintptr) uintptr {
	// a is generally a power of two. This will get inlined and
	// the compiler will optimize the division.
	return (n + a - 1) / a
}

// checkASM reports whether assembly runtime checks have passed.
func checkASM() bool

func memequal_varlen(a, b unsafe.Pointer) bool

// bool2int returns 0 if x is false or 1 if x is true.
func bool2int(x bool) int {
	// Avoid branches. In the SSA compiler, this compiles to
	// exactly what you would want it to.
	return int(uint8(*(*uint8)(unsafe.Pointer(&x))))
}

// abort crashes the runtime in situations where even throw might not
// work. In general it should do something a debugger will recognize
// (e.g., an INT3 on x86). A crash in abort is recognized by the
// signal handler, which will attempt to tear down the runtime
// immediately.
func abort()

// Called from compiled code; declared for vet; do NOT call from Go.
// 写屏障是保证 Go 语言并发标记安全不可获取的技术，我们需要使用混合写屏障维护对象图的弱三色不变性，
// 然而写屏障的实现需要编译器和运行时的共同协作。
// 在 SSA 中间代码生成阶段，编译器会使用 cmd/compile/internal/ssa.writebarrier 函数在 Store、Move 和 Zero 操作中加入写屏障
func gcWriteBarrier()
func duffzero()
func duffcopy()

// Called from linker-generated .initarray; declared for go vet; do NOT call from Go.
func addmoduledata()
