// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// System calls and other sys.stuff for AMD64, Darwin
// System calls are implemented in libSystem, this file contains
// trampolines that convert from Go to C calling convention.

#include "go_asm.h"
#include "go_tls.h"
#include "textflag.h"

// Exit the entire program (like C exit)
TEXT runtime·exit_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	0(DI), DI		// arg 1 exit status
	CALL	libc_exit(SB)
	MOVL	$0xf1, 0xf1  // crash
	POPQ	BP
	RET

TEXT runtime·open_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	8(DI), SI		// arg 2 flags
	MOVL	12(DI), DX		// arg 3 mode
	MOVQ	0(DI), DI		// arg 1 pathname
	XORL	AX, AX			// vararg: say "no float args"
	CALL	libc_open(SB)
	POPQ	BP
	RET

TEXT runtime·close_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	0(DI), DI		// arg 1 fd
	CALL	libc_close(SB)
	POPQ	BP
	RET

TEXT runtime·read_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 buf
	MOVL	16(DI), DX		// arg 3 count
	MOVL	0(DI), DI		// arg 1 fd
	CALL	libc_read(SB)
	TESTL	AX, AX
	JGE	noerr
	CALL	libc_error(SB)
	MOVL	(AX), AX
	NEGL	AX			// caller expects negative errno value
noerr:
	POPQ	BP
	RET

TEXT runtime·write_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 buf
	MOVL	16(DI), DX		// arg 3 count
	MOVQ	0(DI), DI		// arg 1 fd
	CALL	libc_write(SB)
	TESTL	AX, AX
	JGE	noerr
	CALL	libc_error(SB)
	MOVL	(AX), AX
	NEGL	AX			// caller expects negative errno value
noerr:
	POPQ	BP
	RET

TEXT runtime·pipe_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	CALL	libc_pipe(SB)		// pointer already in DI
	TESTL	AX, AX
	JEQ	3(PC)
	CALL	libc_error(SB)		// return negative errno value
	NEGL	AX
	POPQ	BP
	RET

TEXT runtime·setitimer_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 new
	MOVQ	16(DI), DX		// arg 3 old
	MOVL	0(DI), DI		// arg 1 which
	CALL	libc_setitimer(SB)
	POPQ	BP
	RET

TEXT runtime·madvise_trampoline(SB), NOSPLIT, $0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 len
	MOVL	16(DI), DX	// arg 3 advice
	MOVQ	0(DI), DI	// arg 1 addr
	CALL	libc_madvise(SB)
	// ignore failure - maybe pages are locked
	POPQ	BP
	RET

GLOBL timebase<>(SB),NOPTR,$(machTimebaseInfo__size)

TEXT runtime·nanotime_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	DI, BX
	CALL	libc_mach_absolute_time(SB)
	MOVQ	AX, 0(BX)
	MOVL	timebase<>+machTimebaseInfo_numer(SB), SI
	MOVL	timebase<>+machTimebaseInfo_denom(SB), DI // atomic read
	TESTL	DI, DI
	JNE	initialized

	SUBQ	$(machTimebaseInfo__size+15)/16*16, SP
	MOVQ	SP, DI
	CALL	libc_mach_timebase_info(SB)
	MOVL	machTimebaseInfo_numer(SP), SI
	MOVL	machTimebaseInfo_denom(SP), DI
	ADDQ	$(machTimebaseInfo__size+15)/16*16, SP

	MOVL	SI, timebase<>+machTimebaseInfo_numer(SB)
	MOVL	DI, AX
	XCHGL	AX, timebase<>+machTimebaseInfo_denom(SB) // atomic write

initialized:
	MOVL	SI, 8(BX)
	MOVL	DI, 12(BX)
	MOVQ	BP, SP
	POPQ	BP
	RET

TEXT runtime·walltime_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP			// make a frame; keep stack aligned
	MOVQ	SP, BP
	// DI already has *timeval
	XORL	SI, SI // no timezone needed
	CALL	libc_gettimeofday(SB)
	POPQ	BP
	RET

TEXT runtime·sigaction_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 new
	MOVQ	16(DI), DX		// arg 3 old
	MOVL	0(DI), DI		// arg 1 sig
	CALL	libc_sigaction(SB)
	TESTL	AX, AX
	JEQ	2(PC)
	MOVL	$0xf1, 0xf1  // crash
	POPQ	BP
	RET

TEXT runtime·sigprocmask_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 new
	MOVQ	16(DI), DX	// arg 3 old
	MOVL	0(DI), DI	// arg 1 how
	CALL	libc_pthread_sigmask(SB)
	TESTL	AX, AX
	JEQ	2(PC)
	MOVL	$0xf1, 0xf1  // crash
	POPQ	BP
	RET

TEXT runtime·sigaltstack_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 old
	MOVQ	0(DI), DI		// arg 1 new
	CALL	libc_sigaltstack(SB)
	TESTQ	AX, AX
	JEQ	2(PC)
	MOVL	$0xf1, 0xf1  // crash
	POPQ	BP
	RET

TEXT runtime·raiseproc_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	0(DI), BX	// signal
	CALL	libc_getpid(SB)
	MOVL	AX, DI		// arg 1 pid
	MOVL	BX, SI		// arg 2 signal
	CALL	libc_kill(SB)
	POPQ	BP
	RET

TEXT runtime·sigfwd(SB),NOSPLIT,$0-32
	MOVQ	fn+0(FP),    AX
	MOVL	sig+8(FP),   DI
	MOVQ	info+16(FP), SI
	MOVQ	ctx+24(FP),  DX
	PUSHQ	BP
	MOVQ	SP, BP
	ANDQ	$~15, SP     // alignment for x86_64 ABI
	CALL	AX
	MOVQ	BP, SP
	POPQ	BP
	RET

// This is the function registered during sigaction and is invoked when
// a signal is received. It just redirects to the Go function sigtrampgo.
TEXT runtime·sigtramp(SB),NOSPLIT,$0
	// This runs on the signal stack, so we have lots of stack available.
	// We allocate our own stack space, because if we tell the linker
	// how much we're using, the NOSPLIT check fails.
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$64, SP

	// Save callee-save registers.
	MOVQ	BX, 24(SP)
	MOVQ	R12, 32(SP)
	MOVQ	R13, 40(SP)
	MOVQ	R14, 48(SP)
	MOVQ	R15, 56(SP)

	// Call into the Go signal handler
	MOVL	DI, 0(SP)  // sig
	MOVQ	SI, 8(SP)  // info
	MOVQ	DX, 16(SP) // ctx
	CALL runtime·sigtrampgo(SB)

	// Restore callee-save registers.
	MOVQ	24(SP), BX
	MOVQ	32(SP), R12
	MOVQ	40(SP), R13
	MOVQ	48(SP), R14
	MOVQ	56(SP), R15

	MOVQ	BP, SP
	POPQ	BP
	RET

// Used instead of sigtramp in programs that use cgo.
// Arguments from kernel are in DI, SI, DX.
TEXT runtime·cgoSigtramp(SB),NOSPLIT,$0
	// If no traceback function, do usual sigtramp.
	MOVQ	runtime·cgoTraceback(SB), AX
	TESTQ	AX, AX
	JZ	sigtramp

	// If no traceback support function, which means that
	// runtime/cgo was not linked in, do usual sigtramp.
	MOVQ	_cgo_callers(SB), AX
	TESTQ	AX, AX
	JZ	sigtramp

	// Figure out if we are currently in a cgo call.
	// If not, just do usual sigtramp.
	get_tls(CX)
	MOVQ	g(CX),AX
	TESTQ	AX, AX
	JZ	sigtrampnog     // g == nil
	MOVQ	g_m(AX), AX
	TESTQ	AX, AX
	JZ	sigtramp        // g.m == nil
	MOVL	m_ncgo(AX), CX
	TESTL	CX, CX
	JZ	sigtramp        // g.m.ncgo == 0
	MOVQ	m_curg(AX), CX
	TESTQ	CX, CX
	JZ	sigtramp        // g.m.curg == nil
	MOVQ	g_syscallsp(CX), CX
	TESTQ	CX, CX
	JZ	sigtramp        // g.m.curg.syscallsp == 0
	MOVQ	m_cgoCallers(AX), R8
	TESTQ	R8, R8
	JZ	sigtramp        // g.m.cgoCallers == nil
	MOVL	m_cgoCallersUse(AX), CX
	TESTL	CX, CX
	JNZ	sigtramp	// g.m.cgoCallersUse != 0

	// Jump to a function in runtime/cgo.
	// That function, written in C, will call the user's traceback
	// function with proper unwind info, and will then call back here.
	// The first three arguments, and the fifth, are already in registers.
	// Set the two remaining arguments now.
	MOVQ	runtime·cgoTraceback(SB), CX
	MOVQ	$runtime·sigtramp(SB), R9
	MOVQ	_cgo_callers(SB), AX
	JMP	AX

sigtramp:
	JMP	runtime·sigtramp(SB)

sigtrampnog:
	// Signal arrived on a non-Go thread. If this is SIGPROF, get a
	// stack trace.
	CMPL	DI, $27 // 27 == SIGPROF
	JNZ	sigtramp

	// Lock sigprofCallersUse.
	MOVL	$0, AX
	MOVL	$1, CX
	MOVQ	$runtime·sigprofCallersUse(SB), R11
	LOCK
	CMPXCHGL	CX, 0(R11)
	JNZ	sigtramp  // Skip stack trace if already locked.

	// Jump to the traceback function in runtime/cgo.
	// It will call back to sigprofNonGo, which will ignore the
	// arguments passed in registers.
	// First three arguments to traceback function are in registers already.
	MOVQ	runtime·cgoTraceback(SB), CX
	MOVQ	$runtime·sigprofCallers(SB), R8
	MOVQ	$runtime·sigprofNonGo(SB), R9
	MOVQ	_cgo_callers(SB), AX
	JMP	AX

TEXT runtime·mmap_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP			// make a frame; keep stack aligned
	MOVQ	SP, BP
	MOVQ	DI, BX
	MOVQ	0(BX), DI		// arg 1 addr
	MOVQ	8(BX), SI		// arg 2 len
	MOVL	16(BX), DX		// arg 3 prot
	MOVL	20(BX), CX		// arg 4 flags
	MOVL	24(BX), R8		// arg 5 fid
	MOVL	28(BX), R9		// arg 6 offset
	CALL	libc_mmap(SB)
	XORL	DX, DX
	CMPQ	AX, $-1
	JNE	ok
	CALL	libc_error(SB)
	MOVLQSX	(AX), DX		// errno
	XORL	AX, AX
ok:
	MOVQ	AX, 32(BX)
	MOVQ	DX, 40(BX)
	POPQ	BP
	RET

TEXT runtime·munmap_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 len
	MOVQ	0(DI), DI		// arg 1 addr
	CALL	libc_munmap(SB)
	TESTQ	AX, AX
	JEQ	2(PC)
	MOVL	$0xf1, 0xf1  // crash
	POPQ	BP
	RET

TEXT runtime·usleep_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	0(DI), DI	// arg 1 usec
	CALL	libc_usleep(SB)
	POPQ	BP
	RET

TEXT runtime·settls(SB),NOSPLIT,$32
	// Nothing to do on Darwin, pthread already set thread-local storage up.
	RET

TEXT runtime·sysctl_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	8(DI), SI		// arg 2 miblen
	MOVQ	16(DI), DX		// arg 3 out
	MOVQ	24(DI), CX		// arg 4 size
	MOVQ	32(DI), R8		// arg 5 dst
	MOVQ	40(DI), R9		// arg 6 ndst
	MOVQ	0(DI), DI		// arg 1 mib
	CALL	libc_sysctl(SB)
	POPQ	BP
	RET

TEXT runtime·kqueue_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	CALL	libc_kqueue(SB)
	POPQ	BP
	RET

TEXT runtime·kevent_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI		// arg 2 keventt
	MOVL	16(DI), DX		// arg 3 nch
	MOVQ	24(DI), CX		// arg 4 ev
	MOVL	32(DI), R8		// arg 5 nev
	MOVQ	40(DI), R9		// arg 6 ts
	MOVL	0(DI), DI		// arg 1 kq
	CALL	libc_kevent(SB)
	CMPL	AX, $-1
	JNE	ok
	CALL	libc_error(SB)
	MOVLQSX	(AX), AX		// errno
	NEGQ	AX			// caller wants it as a negative error code
ok:
	POPQ	BP
	RET

TEXT runtime·fcntl_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	4(DI), SI		// arg 2 cmd
	MOVL	8(DI), DX		// arg 3 arg
	MOVL	0(DI), DI		// arg 1 fd
	XORL	AX, AX			// vararg: say "no float args"
	CALL	libc_fcntl(SB)
	POPQ	BP
	RET

// mstart_stub is the first function executed on a new thread started by pthread_create.
// It just does some low-level setup and then calls mstart.
// Note: called with the C calling convention.
// 这段代码是 Go 运行时中的 mstart_stub 函数的实现，它是新线程由 pthread_create 启动后执行的第一个函数。
// 它负责一些低级别的初始化工作，然后调用 mstart 函数。
// 该函数使用 C 调用约定（C calling convention）进行调用。
TEXT runtime·mstart_stub(SB),NOSPLIT,$0
	// DI points to the m.
	// We are already on m's g0 stack.

    // 在计算机体系结构中，寄存器是处理器中用于存储临时数据的高速存储单元。
    // 不同的调用约定（calling conventions）规定了函数调用过程中哪些寄存器需要由调用者（caller）保存，哪些寄存器需要由被调用者（callee）保存。
    // 这些约定确保了函数调用的正确性和数据的一致性。

	// Save callee-save registers.
	// 保存 被调用者保存的寄存器（callee-save registers），
	// 也称为非易失性寄存器（non-volatile registers），
	// 是指在函数调用过程中，如果被调用的函数（callee）需要使用这些寄存器，它必须在使用之前保存它们的值，
	// 并在返回之前恢复它们的值。这些寄存器的值在函数调用前后应该保持不变。
	SUBQ	$40, SP
	MOVQ	BX, 0(SP)   // 将 BX 寄存器的值保存到栈上。
	MOVQ	R12, 8(SP)  // 将 R12 寄存器的值保存到栈上。
	MOVQ	R13, 16(SP) // 将 R13 寄存器的值保存到栈上。
	MOVQ	R14, 24(SP) // 将 R14 寄存器的值保存到栈上。
	MOVQ	R15, 32(SP) // 将 R15 寄存器的值保存到栈上。

    // 将 m 结构体中的 g0 字段（表示当前线程的 g0 协程）加载到 DX 寄存器。
	MOVQ	m_g0(DI), DX // g

	// Initialize TLS entry.
	// See cmd/link/internal/ld/sym.go:computeTLSOffset.
	// 将 DX 寄存器的值（即 g0 协程）存储到线程局部存储（TLS）中。0x30(GS) 是 TLS 的偏移量。
	MOVQ	DX, 0x30(GS)

	// Someday the convention will be D is always cleared.
	// 清除方向标志（DF），确保字符串操作指令从低地址向高地址方向处理数据。
	CLD

    // 调用 mstart 函数，mstart 是 Go 运行时中的一个函数，用于启动新的 M（机器，代表一个操作系统线程）。
	CALL	runtime·mstart(SB)

	// Restore callee-save registers.
	// 恢复 被调用者保存的寄存器
	MOVQ	0(SP), BX
	MOVQ	8(SP), R12
	MOVQ	16(SP), R13
	MOVQ	24(SP), R14
	MOVQ	32(SP), R15

    // 当从runtime.mstart调用返回，表示:
    // Go 运行时已经完成了对这个操作系统线程的所有操作。这个线程即将结束，不再需要执行任何 Go 代码。
    // 在通知 pthread 库时，返回值并不重要，因为这个线程是分离状态（detached），不会被其他线程 join（等待其结束）。
    // 在 pthread 库中，线程可以被创建为分离状态，这意味着它们在结束时会自动清理资源，而不需要其他线程显式地等待它们结束。

	// Go is all done with this OS thread.
	// Tell pthread everything is ok (we never join with this thread, so
	// the value here doesn't really matter).
	// 将 AX 寄存器清零。这个值将作为线程的返回值传递给 pthread 库，但由于线程是分离状态（detached），这个值实际上不会被使用。
	XORL	AX, AX
    // 释放之前为保存寄存器分配的栈空间。
	ADDQ	$40, SP
	// 返回，结束当前线程的执行。
	RET

// These trampolines help convert from Go calling convention to C calling convention.
// They should be called with asmcgocall.
// A pointer to the arguments is passed in DI.
// A single int32 result is returned in AX.
// (For more results, make an args/results structure.)
TEXT runtime·pthread_attr_init_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP	// make frame, keep stack 16-byte aligned.
	MOVQ	SP, BP
	MOVQ	0(DI), DI // arg 1 attr
	CALL	libc_pthread_attr_init(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_attr_getstacksize_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 size
	MOVQ	0(DI), DI	// arg 1 attr
	CALL	libc_pthread_attr_getstacksize(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_attr_setdetachstate_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 state
	MOVQ	0(DI), DI	// arg 1 attr
	CALL	libc_pthread_attr_setdetachstate(SB)
	POPQ	BP
	RET

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

TEXT runtime·raise_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVL	0(DI), DI	// arg 1 signal
	CALL	libc_raise(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_mutex_init_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 attr
	MOVQ	0(DI), DI	// arg 1 mutex
	CALL	libc_pthread_mutex_init(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_mutex_lock_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	0(DI), DI	// arg 1 mutex
	CALL	libc_pthread_mutex_lock(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_mutex_unlock_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	0(DI), DI	// arg 1 mutex
	CALL	libc_pthread_mutex_unlock(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_cond_init_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 attr
	MOVQ	0(DI), DI	// arg 1 cond
	CALL	libc_pthread_cond_init(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_cond_wait_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 mutex
	MOVQ	0(DI), DI	// arg 1 cond
	CALL	libc_pthread_cond_wait(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_cond_timedwait_relative_np_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 mutex
	MOVQ	16(DI), DX	// arg 3 timeout
	MOVQ	0(DI), DI	// arg 1 cond
	CALL	libc_pthread_cond_timedwait_relative_np(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_cond_signal_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	0(DI), DI	// arg 1 cond
	CALL	libc_pthread_cond_signal(SB)
	POPQ	BP
	RET

TEXT runtime·pthread_self_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	DI, BX		// BX is caller-save
	CALL	libc_pthread_self(SB)
	MOVQ	AX, 0(BX)	// return value
	POPQ	BP
	RET

TEXT runtime·pthread_kill_trampoline(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	MOVQ	8(DI), SI	// arg 2 sig
	MOVQ	0(DI), DI	// arg 1 thread
	CALL	libc_pthread_kill(SB)
	POPQ	BP
	RET

// syscall calls a function in libc on behalf of the syscall package.
// syscall takes a pointer to a struct like:
// struct {
//	fn    uintptr
//	a1    uintptr
//	a2    uintptr
//	a3    uintptr
//	r1    uintptr
//	r2    uintptr
//	err   uintptr
// }
// syscall must be called on the g0 stack with the
// C calling convention (use libcCall).
//
// syscall expects a 32-bit result and tests for 32-bit -1
// to decide there was an error.
TEXT runtime·syscall(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), CX // fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	CX

	MOVQ	(SP), DI
	MOVQ	AX, (4*8)(DI) // r1
	MOVQ	DX, (5*8)(DI) // r2

	// Standard libc functions return -1 on error
	// and set errno.
	CMPL	AX, $-1	      // Note: high 32 bits are junk
	JNE	ok

	// Get error code from libc.
	CALL	libc_error(SB)
	MOVLQSX	(AX), AX
	MOVQ	(SP), DI
	MOVQ	AX, (6*8)(DI) // err

ok:
	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET

// syscallX calls a function in libc on behalf of the syscall package.
// syscallX takes a pointer to a struct like:
// struct {
//	fn    uintptr
//	a1    uintptr
//	a2    uintptr
//	a3    uintptr
//	r1    uintptr
//	r2    uintptr
//	err   uintptr
// }
// syscallX must be called on the g0 stack with the
// C calling convention (use libcCall).
//
// syscallX is like syscall but expects a 64-bit result
// and tests for 64-bit -1 to decide there was an error.
TEXT runtime·syscallX(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), CX // fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	CX

	MOVQ	(SP), DI
	MOVQ	AX, (4*8)(DI) // r1
	MOVQ	DX, (5*8)(DI) // r2

	// Standard libc functions return -1 on error
	// and set errno.
	CMPQ	AX, $-1
	JNE	ok

	// Get error code from libc.
	CALL	libc_error(SB)
	MOVLQSX	(AX), AX
	MOVQ	(SP), DI
	MOVQ	AX, (6*8)(DI) // err

ok:
	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET

// syscallPtr is like syscallX except that the libc function reports an
// error by returning NULL and setting errno.
TEXT runtime·syscallPtr(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), CX // fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	CX

	MOVQ	(SP), DI
	MOVQ	AX, (4*8)(DI) // r1
	MOVQ	DX, (5*8)(DI) // r2

	// syscallPtr libc functions return NULL on error
	// and set errno.
	TESTQ	AX, AX
	JNE	ok

	// Get error code from libc.
	CALL	libc_error(SB)
	MOVLQSX	(AX), AX
	MOVQ	(SP), DI
	MOVQ	AX, (6*8)(DI) // err

ok:
	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET

// syscall6 calls a function in libc on behalf of the syscall package.
// syscall6 takes a pointer to a struct like:
// struct {
//	fn    uintptr
//	a1    uintptr
//	a2    uintptr
//	a3    uintptr
//	a4    uintptr
//	a5    uintptr
//	a6    uintptr
//	r1    uintptr
//	r2    uintptr
//	err   uintptr
// }
// syscall6 must be called on the g0 stack with the
// C calling convention (use libcCall).
//
// syscall6 expects a 32-bit result and tests for 32-bit -1
// to decide there was an error.
TEXT runtime·syscall6(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), R11// fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	(4*8)(DI), CX // a4
	MOVQ	(5*8)(DI), R8 // a5
	MOVQ	(6*8)(DI), R9 // a6
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	R11

	MOVQ	(SP), DI
	MOVQ	AX, (7*8)(DI) // r1
	MOVQ	DX, (8*8)(DI) // r2

	CMPL	AX, $-1
	JNE	ok

	CALL	libc_error(SB)
	MOVLQSX	(AX), AX
	MOVQ	(SP), DI
	MOVQ	AX, (9*8)(DI) // err

ok:
	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET

// syscall6X calls a function in libc on behalf of the syscall package.
// syscall6X takes a pointer to a struct like:
// struct {
//	fn    uintptr
//	a1    uintptr
//	a2    uintptr
//	a3    uintptr
//	a4    uintptr
//	a5    uintptr
//	a6    uintptr
//	r1    uintptr
//	r2    uintptr
//	err   uintptr
// }
// syscall6X must be called on the g0 stack with the
// C calling convention (use libcCall).
//
// syscall6X is like syscall6 but expects a 64-bit result
// and tests for 64-bit -1 to decide there was an error.
TEXT runtime·syscall6X(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), R11// fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	(4*8)(DI), CX // a4
	MOVQ	(5*8)(DI), R8 // a5
	MOVQ	(6*8)(DI), R9 // a6
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	R11

	MOVQ	(SP), DI
	MOVQ	AX, (7*8)(DI) // r1
	MOVQ	DX, (8*8)(DI) // r2

	CMPQ	AX, $-1
	JNE	ok

	CALL	libc_error(SB)
	MOVLQSX	(AX), AX
	MOVQ	(SP), DI
	MOVQ	AX, (9*8)(DI) // err

ok:
	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET

// syscallNoErr is like syscall6 but does not check for errors, and
// only returns one value, for use with standard C ABI library functions.
TEXT runtime·syscallNoErr(SB),NOSPLIT,$0
	PUSHQ	BP
	MOVQ	SP, BP
	SUBQ	$16, SP
	MOVQ	(0*8)(DI), R11// fn
	MOVQ	(2*8)(DI), SI // a2
	MOVQ	(3*8)(DI), DX // a3
	MOVQ	(4*8)(DI), CX // a4
	MOVQ	(5*8)(DI), R8 // a5
	MOVQ	(6*8)(DI), R9 // a6
	MOVQ	DI, (SP)
	MOVQ	(1*8)(DI), DI // a1
	XORL	AX, AX	      // vararg: say "no float args"

	CALL	R11

	MOVQ	(SP), DI
	MOVQ	AX, (7*8)(DI) // r1

	XORL	AX, AX        // no error (it's ignored anyway)
	MOVQ	BP, SP
	POPQ	BP
	RET
