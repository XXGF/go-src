// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/cpu"
	"runtime/internal/atomic"
	"runtime/internal/sys"
	"unsafe"
)

var buildVersion = sys.TheVersion

// set using cmd/go/internal/modload.ModInfoProg
var modinfo string

// Goroutine scheduler
// The scheduler's job is to distribute ready-to-run goroutines over worker threads.
//
// The main concepts are:
// G - goroutine.
// M - worker thread, or machine.
// P - processor, a resource that is required to execute Go code.
//     M must have an associated P to execute Go code, however it can be
//     blocked or in a syscall w/o an associated P.
//
// Design doc at https://golang.org/s/go11sched.

/*
Go 运行时系统中关于工作线程（worker thread）停放（parking）和唤醒（unparking）的策略。
其核心目标是在充分利用硬件并行性的同时，避免过多的线程运行以节省 CPU 资源和功耗。以下是对这段注释的详细解释：

背景
	在 Go 的调度器中，工作线程（M）与处理器（P）和 Goroutine（G）协同工作。
	调度器需要在保持足够的运行线程以利用硬件并行性和停放多余的线程以节省资源之间取得平衡。

挑战
	1. 分布式调度器状态：调度器状态是分布式的，特别是每个 P 的工作队列。这使得在快速路径上计算全局状态变得困难。
	2. 未来的不确定性：为了实现最佳的线程管理，我们需要知道未来的情况（例如，不要在即将有新的 Goroutine 准备就绪时停放线程）。

被拒绝的三种方法
	1. 集中化调度器状态：这会抑制系统的可扩展性。
	2. 直接 Goroutine 交接：当一个新的 Goroutine 准备就绪且有空闲的 P 时，唤醒一个线程并交接 Goroutine。这会导致线程状态的频繁切换，并破坏计算的局部性。【一个G新启另一个G，他们之间的数据往往是有关联的，应该将它们放在同一个M中执行，以利用好计算的局部性】
	3. 每次准备 Goroutine 时唤醒额外线程：这会导致过多的线程停放和唤醒，因为额外的线程可能会立即停放而没有发现任何工作。

当前的方法
	1. 当一个新的 Goroutine 准备就绪时，如果有空闲的 P 且没有“自旋”的工作线程，则唤醒一个额外的线程。
	2. 自旋线程是指那些没有本地工作且在全局运行队列或网络轮询器中没有找到工作的线程。
	3. 自旋线程在停放（parking）前会在每个 P 的运行队列中寻找工作。
	4. 如果自旋线程找到工作，它会退出自旋状态并继续执行；如果没有找到工作，它会退出自旋状态然后停放。
	5. 如果至少有一个自旋线程（sched.nmspinning > 1），在准备 Goroutine 时不会唤醒新线程。
	6. 如果最后一个自旋线程找到工作并停止自旋，它必须唤醒一个新的自旋线程。

实现复杂性
	1. 在自旋到非自旋线程的转换过程中需要非常小心。这种转换可能与新 Goroutine 的提交竞争。
	2. Goroutine 准备的通用模式是：将 Goroutine 提交到本地工作队列，进行内存屏障操作，然后检查 sched.nmspinning。
	3. 自旋到非自旋转换的通用模式是：减少 nmspinning，进行内存屏障操作，然后检查所有 P 的工作队列以寻找新工作。
总结
	这种方法通过平滑 不必要的线程唤醒峰值，同时保证最终的最大 CPU 并行性利用率。通过小心处理自旋到非自旋的转换，避免了 CPU 资源的半持久性未充分利用。
*/

// Worker thread parking/unparking.
// We need to balance between keeping enough running worker threads to utilize
// available hardware parallelism and parking excessive running worker threads
// to conserve CPU resources and power. This is not simple for two reasons:
// (1) scheduler state is intentionally distributed (in particular, per-P work
// queues), so it is not possible to compute global predicates on fast paths;
// (2) for optimal thread management we would need to know the future (don't park
// a worker thread when a new goroutine will be readied in near future).
//
// Three rejected approaches that would work badly:
// 1. Centralize all scheduler state (would inhibit scalability).
// 2. Direct goroutine handoff. That is, when we ready a new goroutine and there
//    is a spare P, unpark a thread and handoff it the thread and the goroutine.
//    This would lead to thread state thrashing, as the thread that readied the
//    goroutine can be out of work the very next moment, we will need to park it.
//    Also, it would destroy locality of computation as we want to preserve
//    dependent goroutines on the same thread; and introduce additional latency.
// 3. Unpark an additional thread whenever we ready a goroutine and there is an
//    idle P, but don't do handoff. This would lead to excessive thread parking/
//    unparking as the additional threads will instantly park without discovering
//    any work to do.
//
// The current approach:
// We unpark an additional thread when we ready a goroutine if (1) there is an
// idle P and there are no "spinning" worker threads. A worker thread is considered
// spinning if it is out of local work and did not find work in global run queue/
// netpoller; the spinning state is denoted in m.spinning and in sched.nmspinning.
// Threads unparked this way are also considered spinning; we don't do goroutine
// handoff so such threads are out of work initially. Spinning threads do some
// spinning looking for work in per-P run queues before parking. If a spinning
// thread finds work it takes itself out of the spinning state and proceeds to
// execution. If it does not find work it takes itself out of the spinning state
// and then parks.
// If there is at least one spinning thread (sched.nmspinning>1), we don't unpark
// new threads when readying goroutines. To compensate for that, if the last spinning
// thread finds work and stops spinning, it must unpark a new spinning thread.
// This approach smooths out unjustified spikes of thread unparking,
// but at the same time guarantees eventual maximal CPU parallelism utilization.
//
// The main implementation complication is that we need to be very careful during
// spinning->non-spinning thread transition. This transition can race with submission
// of a new goroutine, and either one part or another needs to unpark another worker
// thread. If they both fail to do that, we can end up with semi-persistent CPU
// underutilization. The general pattern for goroutine readying is: submit a goroutine
// to local work queue, #StoreLoad-style memory barrier, check sched.nmspinning.
// The general pattern for spinning->non-spinning transition is: decrement nmspinning,
// #StoreLoad-style memory barrier, check all per-P work queues for new work.
// Note that all this complexity does not apply to global run queue as we are not
// sloppy about thread unparking when submitting to global queue. Also see comments
// for nmspinning manipulation.

var (
	m0           m
	g0           g
	mcache0      *mcache
	raceprocctx0 uintptr
)

//go:linkname runtime_inittask runtime..inittask
var runtime_inittask initTask

//go:linkname main_inittask main..inittask
var main_inittask initTask

/*
	main_init_done 是一个布尔类型的通道，用于在初始化过程中传递信号。
	这个通道用于通知 cgocallbackg（一个与 cgo 回调相关的函数）初始化过程已经完成。

	注释解释了 main_init_done 的作用和使用时机：
		main_init_done 是一个信号，用于通知 cgocallbackg 初始化已经完成。
		这个通道在 _cgo_notify_runtime_init_done 之前创建，因此所有 cgo 调用都可以依赖它的存在。
		当 main_init 完成时，这个通道会被关闭，这意味着 cgocallbackg 可以可靠地从这个通道接收信号。

*/
// main_init_done is a signal used by cgocallbackg that initialization
// has been completed. It is made before _cgo_notify_runtime_init_done,
// so all cgo calls can rely on it existing. When main_init is complete,
// it is closed, meaning cgocallbackg can reliably receive from it.
var main_init_done chan bool

//go:linkname main_main main.main
func main_main()

/*
	mainStarted 只在runtime.main函数中，被设置为true的。
	mainStarted 是一个全局变量，用于指示主 goroutine 是否已经启动。这个变量通常用于控制某些初始化操作的执行顺序，确保在主 goroutine 启动之前或之后执行特定的操作。
	mainStarted 变量在 Go 运行时中有几个关键的用途：
	1. 初始化控制：在 Go 运行时初始化过程中，有些操作需要在主 goroutine 启动之前完成，而有些操作则需要在主 goroutine 启动之后进行。mainStarted 变量用于区分这两种情况。
	2. 调度器行为：在 Go 的调度器中，mainStarted 变量可以用于控制调度器的行为。例如，在主 goroutine 启动之前，调度器可能需要执行一些特殊的初始化操作。
*/
// mainStarted indicates that the main M has started.
var mainStarted bool

// runtimeInitTime is the nanotime() at which the runtime started.
var runtimeInitTime int64

// Value to use for signal mask for newly created M's.
var initSigmask sigset

/*
	在 Go 程序启动时，runtime.main 函数会被调用来初始化运行时环境。
	这个函数会创建和启动第一个 goroutine（即用户定义的 main.main 函数）。在这个过程中，runtime.main 会执行一些初始化操作

	runtime.main 函数通常在 Go 程序的启动过程中被调用。
	具体来说，它是在 Go 程序的入口点（如 _rt0_arm64_darwin 或 _rt0_amd64_linux 等）中被调用的。
*/
// The main goroutine.
func main() {
	// getg 是一个内联汇编函数，用于获取当前正在运行的 goroutine 的指针。
	// 在 Go 运行时系统中，每个 goroutine 都有一个 g 结构体表示。
	g := getg()

	/*
		获取当前 goroutine 所属的 M（操作系统线程）。
		获取 M 的第一个 goroutine（通常是 g0，负责调度和管理其他 goroutine）。
		m.g0.racectx 是一个用于数据竞争检测的上下文（race context）。
	*/
	// Racectx of m0->g0 is used only as the parent of the main goroutine.
	// It must not be used for anything else.
	g.m.g0.racectx = 0

	/*
		用于设置 goroutine 的最大栈大小。不同架构（64 位和 32 位）有不同的最大栈大小限制。
	*/
	// 在 64 位系统上，最大栈大小是 1 GB。
	// 在 32 位系统上，最大栈大小是 250 MB。
	// sys.PtrSize 是一个常量，表示指针的大小（以字节为单位）。
	// 在 64 位系统上，指针大小是 8 字节；在 32 位系统上，指针大小是 4 字节。
	// 通过检查 sys.PtrSize，可以确定当前运行的系统是 64 位还是 32 位。
	// Max stack size is 1 GB on 64-bit, 250 MB on 32-bit.
	// Using decimal instead of binary GB and MB because
	// they look nicer in the stack overflow failure message.
	if sys.PtrSize == 8 {
		maxstacksize = 1000000000
	} else {
		maxstacksize = 250000000
	}

	// Allow newproc to start new Ms.
	// mainStarted 设置为true，表示主协程已启动。
	// 主协程启动之后，才允许 newproc 函数启动新的 M（操作系统线程）。
	mainStarted = true

	/*
		这行代码检查当前的目标架构是否为 WebAssembly（wasm）。
		如果当前的目标架构是 wasm，则不启动系统监控线程。因为在 wasm 上还没有线程的支持，所以不需要系统监控线程。
	*/
	// 在 wasm 目标架构上不启动系统监控线程。因为在 wasm 上还没有线程的支持，所以不需要系统监控线程。
	if GOARCH != "wasm" { // no threads on wasm yet, so no sysmon
		// systemstack 是一个 Go 运行时函数，用于在系统栈上执行指定的函数。系统栈是 Go 运行时用于执行一些关键操作的栈，与 goroutine 的用户栈不同。
		// 这里使用 systemstack 确保 newm 函数在系统栈上执行。
		systemstack(func() {
			// GC的关键组件2：启动系统监控
			// 创建一个新的 M，并在该 M 上运行 sysmon 函数。sysmon 是系统监控函数，负责监控和管理 Go 运行时系统的各种资源和状态。
			// sysmon 是 Go 运行时中的一个关键函数，负责系统监控。它会定期检查和管理 Go 运行时系统的状态，包括垃圾回收（GC）、调度器状态、网络轮询等。
			newm(sysmon, nil)
		})
	}

	/*
			lockOSThread 是 Go 运行时中的一个函数，用于将当前的 goroutine 锁定到当前的操作系统线程上。
			一旦调用 lockOSThread，该 goroutine 将始终在同一个操作系统线程上运行，直到调用 runtime.UnlockOSThread。

			使用场景:
			1. GUI 应用程序：在某些 GUI 库中，所有的 UI 操作必须在主线程上执行。通过锁定主 goroutine 到主线程，可以确保这些操作在正确的线程上下文中进行。
			2. 特定的系统调用：某些系统调用可能要求在特定的线程上执行，尤其是在与外部库交互时。
			3. 线程局部存储（TLS）：如果某些库使用线程局部存储来保存状态，锁定 goroutine 到特定线程可以确保状态的一致性

			场景补充：
			1. 与操作系统线程相关的操作
		   某些操作需要在特定的操作系统线程上执行。例如，某些系统调用或库函数可能要求在调用线程上执行后续操作。在这种情况下，Go 运行时系统会将 Goroutine 锁定到特定的 M，以确保这些操作在同一个线程上执行。

			2. 线程本地存储（TLS）
		   某些库或系统调用依赖于线程本地存储（TLS）。为了确保 Goroutine 在同一个线程上执行并访问正确的 TLS 数据，Go 运行时系统会将 Goroutine 锁定到特定的 M。

			3. 外部 C 代码的调用
		   通过 cgo 调用外部 C 代码时，可能需要将 Goroutine 锁定到特定的 M，以确保 C 代码在同一个线程上执行。这是因为某些 C 库可能依赖于线程上下文。

			4. 处理信号
		   在处理操作系统信号时，可能需要将 Goroutine 锁定到特定的 M，以确保信号处理程序在同一个线程上执行。

			5. 其他需要线程亲和性的情况
		   某些情况下，可能需要线程亲和性（Thread Affinity），即确保某些操作在特定的线程上执行，以提高性能或满足特定的需求。在这种情况下，Go 运行时系统会将 Goroutine 锁定到特定的 M。

	*/
	// 锁定主 goroutine：注释说明了在初始化期间，将主 goroutine 锁定到主操作系统线程上。
	// 大多数程序不需要：大多数 Go 程序不需要关心这一点，因为它们不依赖于特定的线程上下文。
	// 特定调用的要求：某些程序可能需要在主线程上执行特定的调用，例如与 GUI 库交互时，通常要求在主线程上进行操作。
	// 通过 runtime.LockOSThread：程序可以通过在初始化期间调用 runtime.LockOSThread 来确保 main.main 在主线程上运行，从而保持这种锁定。
	// Lock the main goroutine onto this, the main OS thread,
	// during initialization. Most programs won't care, but a few
	// do require certain calls to be made by the main thread.
	// Those can arrange for main.main to run in the main thread
	// by calling runtime.LockOSThread during initialization
	// to preserve the lock.
	lockOSThread()

	if g.m != &m0 {
		throw("runtime.main not on m0")
	}

	/*
		在 Go 语言中，包的初始化顺序是非常重要的。
		每个包可以包含一个或多个 init 函数，这些函数在包被首次导入时自动执行。
		doInit 函数负责调用这些 init 函数，并确保它们按照正确的顺序执行。
	*/
	// doInit 函数负责执行 runtime_inittask 中定义的初始化任务。这些任务包括调用包的 init 函数和初始化包级别的变量。
	// 通过调用 doInit 函数，运行时系统可以确保所有包级别的变量和 init 函数都已正确初始化。
	doInit(&runtime_inittask) // must be before defer
	if nanotime() == 0 {
		throw("nanotime returning zero")
	}

	// Defer unlock so that runtime.Goexit during init does the unlock too.
	needUnlock := true
	defer func() {
		if needUnlock {
			unlockOSThread()
		}
	}()

	// 这行代码的作用是记录程序启动的时间点。
	// Record when the world started.
	runtimeInitTime = nanotime()

	// GC的关键组件3：启动垃圾回收器后台
	gcenable()

	/*
		这段代码片段涉及到 Go 运行时系统的初始化过程，特别是与 cgo 相关的初始化。cgo 是 Go 语言中的一个特性，允许 Go 代码调用 C 代码。
	*/
	// main_init_done = make(chan bool) 创建了一个布尔类型的通道，用于通知主初始化过程的完成。
	main_init_done = make(chan bool)
	// iscgo 是一个布尔变量，指示当前程序是否使用了 cgo。如果 iscgo 为 true，则表示程序中包含了 cgo 代码。
	if iscgo {
		// _cgo_thread_start：用于启动 cgo 线程。
		if _cgo_thread_start == nil {
			throw("_cgo_thread_start missing")
		}
		// _cgo_setenv 和 _cgo_unsetenv：用于设置和取消设置环境变量（在非 Windows 系统中）。
		if GOOS != "windows" {
			if _cgo_setenv == nil {
				throw("_cgo_setenv missing")
			}
			if _cgo_unsetenv == nil {
				throw("_cgo_unsetenv missing")
			}
		}
		// _cgo_notify_runtime_init_done：用于通知运行时初始化完成。
		if _cgo_notify_runtime_init_done == nil {
			throw("_cgo_notify_runtime_init_done missing")
		}
		// Start the template thread in case we enter Go from
		// a C-created thread and need to create a new thread.
		// startTemplateThread() 函数用于启动一个模板线程，以防在从 C 创建的线程进入 Go 时需要创建一个新线程。
		startTemplateThread()
		// cgocall(_cgo_notify_runtime_init_done, nil) 调用 _cgo_notify_runtime_init_done 函数，通知运行时系统初始化已完成。
		cgocall(_cgo_notify_runtime_init_done, nil)
	}

	/*
		在 Go 语言中，初始化过程是一个重要的步骤，确保所有包和全局变量在程序开始执行之前都已正确初始化。
		Go 运行时系统使用初始化任务来管理这个过程。每个包都有一个初始化任务，表示该包的初始化过程。
		doInit 函数负责执行这些初始化任务，并确保所有依赖的任务都已完成。例如，如果包 A 依赖于包 B，那么在初始化包 A 之前，必须先初始化包 B。
	*/
	// 用户代码 main.init 和 main.main 入口
	doInit(&main_inittask)

	// cgocallbackg 函数中会监听 main_init_done 这个channel。这里返回之后，cgocallbackg 函数就能从监听channel的阻塞中返回。
	close(main_init_done)

	needUnlock = false
	unlockOSThread()

	// 这行代码检查两个布尔变量 isarchive 和 islibrary 是否为 true。
	// isarchive 和 islibrary 通常用于指示当前编译模式是否为 c-archive 或 c-shared。
	// 如果当前编译模式是 c-archive 或 c-shared，则直接返回，不执行后续的代码。
	if isarchive || islibrary {
		// A program compiled with -buildmode=c-archive or c-shared
		// has a main, but it is not executed.
		return
	}
	// 这行代码将 main_main 函数赋值给变量 fn。通过这种方式，可以间接调用 main_main 函数。
	// 这样做的原因：链接器在布置运行时系统时不知道 main 包的地址，因此需要通过间接调用的方式来调用 main_main 函数。
	fn := main_main // make an indirect call, as the linker doesn't know the address of the main package when laying down the runtime
	// 这行代码通过变量 fn 调用 main_main 函数。 main_main 函数通常是用户定义的 main 函数的入口点。
	fn()
	// raceenabled 是一个布尔变量，用于指示是否启用了数据竞争检测（race detection）。
	if raceenabled {
		// 如果启用了数据竞争检测，则调用 racefini() 函数进行相关的清理工作。
		racefini()
	}

	/*
		在主 goroutine 返回时，如果其他 goroutine 正在 panic，则让这些 goroutine 完成 panic trace 的打印工作。
		一旦完成，它们将退出。这个处理是为了修复一些特定的问题（issues 3934 和 20018）。
	*/
	// Make racy client program work: if panicking on
	// another goroutine at the same time as main returns,
	// let the other goroutine finish printing the panic trace.
	// Once it does, it will exit. See issues 3934 and 20018.
	// runningPanicDefers 是一个原子变量，用于跟踪是否有 goroutine 正在运行 deferred 函数。
	// 如果有 goroutine 正在运行 deferred 函数，则进入循环等待这些函数完成。
	if atomic.Load(&runningPanicDefers) != 0 {
		// Running deferred functions should not take long.
		// 循环最多等待 1000 次，每次检查 runningPanicDefers 是否变为 0。
		for c := 0; c < 1000; c++ {
			if atomic.Load(&runningPanicDefers) == 0 {
				break
			}
			// 使用 Gosched() 让出当前 goroutine 的时间片，以便其他 goroutine 有机会运行。
			/*
				为什么要调用 Gosched()，让出当前goroutine的执行权？

				答：
				这段代码的目的是解决在某些情况下，当主 goroutine 返回时，其他 goroutine 可能正在处理 panic 并打印 panic trace 的问题。
				为了确保这些 goroutine 能够完成 panic trace 的打印，代码会检查是否有 goroutine 正在处理 panic defer 函数。

				调用 Gosched() 的目的是为了确保其他正在处理 panic defer 函数的 goroutine 有机会运行并完成它们的工作。
				避免忙等待：
					如果不调用 Gosched()，当前 goroutine 会在循环中不断检查 runningPanicDefers 的值，这会导致忙等待，占用 CPU 资源。
					通过调用 Gosched()，当前 goroutine 让出处理器时间片，避免了忙等待，提高了 CPU 的利用效率。
			*/
			Gosched()
		}
	}
	// panicking 是一个原子变量，用于跟踪是否有 goroutine 正在 panic。
	// 如果有 goroutine 正在 panic，则调用 gopark 进入等待状态，直到 panic 处理完成。
	if atomic.Load(&panicking) != 0 {
		gopark(nil, nil, waitReasonPanicWait, traceEvGoStop, 1)
	}
	// 调用 exit(0) 退出程序，返回状态码 0。
	exit(0)

	// 死循环部分是为了确保程序在退出前不会继续执行其他代码。
	// 这段代码实际上不会被执行到，因为 exit(0) 会终止程序。
	// 如果exit(0) 没有成功退出程序，这里就通过不断往空指针写入数据，触发空指针panic来强制退出程序？
	/*
		目的和作用
		1. 防止编译器警告：
			在某些情况下，编译器可能会对没有返回值或没有明确终止的函数发出警告。通过添加一个死循环，可以确保编译器不会发出这些警告，因为从编译器的角度来看，函数有一个明确的终止路径。
		2. 防止意外执行：
			虽然 exit(0) 通常会终止程序，但在极少数情况下，可能会有一些意外情况导致程序继续执行。添加一个死循环可以确保即使 exit(0) 失败，程序也不会继续执行其他代码，从而避免潜在的未定义行为或错误。
		3. 代码完整性：
			在某些代码库中，特别是涉及到低级别系统编程或运行时系统的代码，确保代码的完整性和健壮性是非常重要的。通过添加一个死循环，可以确保在任何情况下，程序都不会继续执行到不应该执行的代码。
		4. 调试和诊断：
			在调试和诊断过程中，死循环可以作为一个明确的标记，表明程序在这个点应该已经终止。如果程序在这个点继续执行，可以帮助开发者快速定位问题。
	*/
	for {
		var x *int32
		*x = 0
	}
}

// os_beforeExit is called from os.Exit(0).
//go:linkname os_beforeExit os.runtime_beforeExit
func os_beforeExit() {
	if raceenabled {
		racefini()
	}
}

// start forcegc helper goroutine
func init() {
	// 运行时会在应用程序启动时在后台开启一个用于强制触发垃圾收集的 Goroutine，
	// 该 Goroutine 的职责非常简单 — 调用 runtime.gcStart 方法尝试启动新一轮的垃圾收集
	go forcegchelper()
}

// GC关键组件2： GC 的 forcegc 开始被初始化
// 为了减少对计算资源的占用，该 Goroutine 会在循环中调用 runtime.goparkunlock 主动陷入休眠等待其他 Goroutine 的唤醒，
// runtime.forcegchelper 在大多数时间都是陷入休眠的，但是它会被系统监控器 runtime.sysmon 在满足垃圾收集条件时唤醒：
func forcegchelper() {
	// 指定 forcegc 的 goroutine
	forcegc.g = getg()
	lockInit(&forcegc.lock, lockRankForcegc)
	for {
		lock(&forcegc.lock)
		if forcegc.idle != 0 {
			throw("forcegc: phase error")
		}
		// 将 forcegc 设置为空闲状态，并进入休眠
		atomic.Store(&forcegc.idle, 1)
		goparkunlock(&forcegc.lock, waitReasonForceGCIdle, traceEvGoBlock, 1)
		// this goroutine is explicitly resumed by sysmon
		if debug.gctrace > 0 {
			println("GC forced")
		}
		// Time-triggered, fully concurrent.
		// 当 forcegc.g 被唤醒时，开始从此处进行调度完全并发
		gcStart(gcTrigger{kind: gcTriggerTime, now: nanotime()})
	}
}

//go:nosplit

/*
	Gosched 是 Go 语言运行时中的一个函数，用于让出当前 goroutine 的处理器时间片，从而允许其他 goroutine 运行。
	它不会挂起当前 goroutine，因此当前 goroutine 会在稍后自动恢复执行。
*/
// Gosched yields the processor, allowing other goroutines to run. It does not
// suspend the current goroutine, so execution resumes automatically.
func Gosched() {
	// checkTimeouts 是一个内部函数，用于检查是否有超时的操作需要处理。
	checkTimeouts()
	// mcall 是一个低级别的运行时函数，用于调用特定的调度器函数。在这里，它调用了 gosched_m 函数。
	// gosched_m 是一个内部函数，负责实际的调度操作。它会将当前 goroutine 放回到调度队列中，并选择另一个 goroutine 运行。
	mcall(gosched_m)
}

// goschedguarded yields the processor like gosched, but also checks
// for forbidden states and opts out of the yield in those cases.
//go:nosplit
func goschedguarded() {
	mcall(goschedguarded_m)
}

// Puts the current goroutine into a waiting state and calls unlockf.
// If unlockf returns false, the goroutine is resumed.
// unlockf must not access this G's stack, as it may be moved between
// the call to gopark and the call to unlockf.
// Reason explains why the goroutine has been parked.
// It is displayed in stack traces and heap dumps.
// Reasons should be unique and descriptive.
// Do not re-use reasons, add new ones.
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, traceEv byte, traceskip int) {
	if reason != waitReasonSleep {
		checkTimeouts() // timeouts may expire while two goroutines keep the scheduler busy
	}
	mp := acquirem()
	gp := mp.curg
	status := readgstatus(gp)
	if status != _Grunning && status != _Gscanrunning {
		throw("gopark: bad g status")
	}
	mp.waitlock = lock
	mp.waitunlockf = unlockf
	gp.waitreason = reason
	mp.waittraceev = traceEv
	mp.waittraceskip = traceskip
	releasem(mp)
	// can't do anything that might move the G between Ms here.
	mcall(park_m)
}

// Puts the current goroutine into a waiting state and unlocks the lock.
// The goroutine can be made runnable again by calling goready(gp).
func goparkunlock(lock *mutex, reason waitReason, traceEv byte, traceskip int) {
	gopark(parkunlock_c, unsafe.Pointer(lock), reason, traceEv, traceskip)
}

func goready(gp *g, traceskip int) {
	systemstack(func() {
		ready(gp, traceskip, true)
	})
}

//go:nosplit
func acquireSudog() *sudog {
	// Delicate dance: the semaphore implementation calls
	// acquireSudog, acquireSudog calls new(sudog),
	// new calls malloc, malloc can call the garbage collector,
	// and the garbage collector calls the semaphore implementation
	// in stopTheWorld.
	// Break the cycle by doing acquirem/releasem around new(sudog).
	// The acquirem/releasem increments m.locks during new(sudog),
	// which keeps the garbage collector from being invoked.
	mp := acquirem()
	pp := mp.p.ptr()
	if len(pp.sudogcache) == 0 {
		lock(&sched.sudoglock)
		// First, try to grab a batch from central cache.
		for len(pp.sudogcache) < cap(pp.sudogcache)/2 && sched.sudogcache != nil {
			s := sched.sudogcache
			sched.sudogcache = s.next
			s.next = nil
			pp.sudogcache = append(pp.sudogcache, s)
		}
		unlock(&sched.sudoglock)
		// If the central cache is empty, allocate a new one.
		if len(pp.sudogcache) == 0 {
			pp.sudogcache = append(pp.sudogcache, new(sudog))
		}
	}
	n := len(pp.sudogcache)
	s := pp.sudogcache[n-1]
	pp.sudogcache[n-1] = nil
	pp.sudogcache = pp.sudogcache[:n-1]
	if s.elem != nil {
		throw("acquireSudog: found s.elem != nil in cache")
	}
	releasem(mp)
	return s
}

//go:nosplit
func releaseSudog(s *sudog) {
	if s.elem != nil {
		throw("runtime: sudog with non-nil elem")
	}
	if s.isSelect {
		throw("runtime: sudog with non-false isSelect")
	}
	if s.next != nil {
		throw("runtime: sudog with non-nil next")
	}
	if s.prev != nil {
		throw("runtime: sudog with non-nil prev")
	}
	if s.waitlink != nil {
		throw("runtime: sudog with non-nil waitlink")
	}
	if s.c != nil {
		throw("runtime: sudog with non-nil c")
	}
	gp := getg()
	if gp.param != nil {
		throw("runtime: releaseSudog with non-nil gp.param")
	}
	mp := acquirem() // avoid rescheduling to another P
	pp := mp.p.ptr()
	if len(pp.sudogcache) == cap(pp.sudogcache) {
		// Transfer half of local cache to the central cache.
		var first, last *sudog
		for len(pp.sudogcache) > cap(pp.sudogcache)/2 {
			n := len(pp.sudogcache)
			p := pp.sudogcache[n-1]
			pp.sudogcache[n-1] = nil
			pp.sudogcache = pp.sudogcache[:n-1]
			if first == nil {
				first = p
			} else {
				last.next = p
			}
			last = p
		}
		lock(&sched.sudoglock)
		last.next = sched.sudogcache
		sched.sudogcache = first
		unlock(&sched.sudoglock)
	}
	pp.sudogcache = append(pp.sudogcache, s)
	releasem(mp)
}

/*
	funcPC 是一个 Go 语言中的函数，用于获取函数值 f 的入口程序计数器（PC）。
	这个函数假设 f 是一个函数值，否则其行为是未定义的。
	需要注意的是，在使用插件的程序中，funcPC 可能会返回不同的值，即使是同一个函数，因为在地址空间中可能存在多个相同函数的副本。
	因此，不能将 funcPC 的结果用于 == 表达式中，只能将其结果用作开始执行代码的地址。
*/
// funcPC returns the entry PC of the function f.
// It assumes that f is a func value. Otherwise the behavior is undefined.
// CAREFUL: In programs with plugins, funcPC can return different values
// for the same function (because there are actually multiple copies of
// the same function in the address space). To be safe, don't use the
// results of this function in any == expression. It is only safe to
// use the result as an address at which to start executing code.
//go:nosplit
func funcPC(f interface{}) uintptr {
	// efaceOf 是一个内部函数，用于获取接口值的底层表示。efaceOf(&f).data 返回接口值是 f 的数据部分的指针。
	// 将数据部分的指针转换为 uintptr 类型，并解引用以获取函数的入口程序计数器（PC）。
	return *(*uintptr)(efaceOf(&f).data)
}

// called from assembly
func badmcall(fn func(*g)) {
	throw("runtime: mcall called on m->g0 stack")
}

func badmcall2(fn func(*g)) {
	throw("runtime: mcall function returned")
}

func badreflectcall() {
	panic(plainError("arg size to reflect.call more than 1GB"))
}

var badmorestackg0Msg = "fatal: morestack on g0\n"

//go:nosplit
//go:nowritebarrierrec
func badmorestackg0() {
	sp := stringStructOf(&badmorestackg0Msg)
	write(2, sp.str, int32(sp.len))
}

var badmorestackgsignalMsg = "fatal: morestack on gsignal\n"

//go:nosplit
//go:nowritebarrierrec
func badmorestackgsignal() {
	sp := stringStructOf(&badmorestackgsignalMsg)
	write(2, sp.str, int32(sp.len))
}

//go:nosplit
func badctxt() {
	throw("ctxt != 0")
}

func lockedOSThread() bool {
	gp := getg()
	return gp.lockedm != 0 && gp.m.lockedg != 0
}

/*
	allgs 是一个包含所有曾经创建过的 Goroutine（简称 G）的切片。
	这个切片包括了所有状态的 Goroutine，包括已经完成执行的（dead）Goroutine。由于 allgs 包含了所有曾经创建过的 Goroutine，因此它的大小只会增加，不会缩小。

	注意：这个不是可运行G的全局队列。
*/
var (
	allgs    []*g
	allglock mutex
)

// 用于将一个新的 goroutine 结构体 g 添加到全局的 goroutine 列表中。
func allgadd(gp *g) {
	if readgstatus(gp) == _Gidle {
		throw("allgadd: bad status Gidle")
	}

	lock(&allglock)
	allgs = append(allgs, gp)
	allglen = uintptr(len(allgs))
	unlock(&allglock)
}

const (
	// Number of goroutine ids to grab from sched.goidgen to local per-P cache at once.
	// 16 seems to provide enough amortization, but other than that it's mostly arbitrary number.
	_GoidCacheBatch = 16
)

// cpuinit extracts the environment variable GODEBUG from the environment on
// Unix-like operating systems and calls internal/cpu.Initialize.
func cpuinit() {
	const prefix = "GODEBUG="
	var env string

	switch GOOS {
	case "aix", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "illumos", "solaris", "linux":
		cpu.DebugOptions = true

		// Similar to goenv_unix but extracts the environment value for
		// GODEBUG directly.
		// TODO(moehrmann): remove when general goenvs() can be called before cpuinit()
		n := int32(0)
		for argv_index(argv, argc+1+n) != nil {
			n++
		}

		for i := int32(0); i < n; i++ {
			p := argv_index(argv, argc+1+i)
			s := *(*string)(unsafe.Pointer(&stringStruct{unsafe.Pointer(p), findnull(p)}))

			if hasPrefix(s, prefix) {
				env = gostring(p)[len(prefix):]
				break
			}
		}
	}

	cpu.Initialize(env)

	// Support cpu feature variables are used in code generated by the compiler
	// to guard execution of instructions that can not be assumed to be always supported.
	x86HasPOPCNT = cpu.X86.HasPOPCNT
	x86HasSSE41 = cpu.X86.HasSSE41
	x86HasFMA = cpu.X86.HasFMA

	armHasVFPv4 = cpu.ARM.HasVFPv4

	arm64HasATOMICS = cpu.ARM64.HasATOMICS
}

// The bootstrap sequence is:
//
//	call osinit
//	call schedinit
//	make & queue new G
//	call runtime·mstart
//
// The new G calls runtime·main.
// 运行时通过 runtime.schedinit 函数初始化调度器
func schedinit() {
	lockInit(&sched.lock, lockRankSched)
	lockInit(&sched.sysmonlock, lockRankSysmon)
	lockInit(&sched.deferlock, lockRankDefer)
	lockInit(&sched.sudoglock, lockRankSudog)
	lockInit(&deadlock, lockRankDeadlock)
	lockInit(&paniclk, lockRankPanic)
	lockInit(&allglock, lockRankAllg)
	lockInit(&allpLock, lockRankAllp)
	lockInit(&reflectOffs.lock, lockRankReflectOffs)
	lockInit(&finlock, lockRankFin)
	lockInit(&trace.bufLock, lockRankTraceBuf)
	lockInit(&trace.stringsLock, lockRankTraceStrings)
	lockInit(&trace.lock, lockRankTrace)
	lockInit(&cpuprof.lock, lockRankCpuprof)
	lockInit(&trace.stackTab.lock, lockRankTraceStackTab)

	// raceinit must be the first call to race detector.
	// In particular, it must be done before mallocinit below calls racemapshadow.
	_g_ := getg()
	if raceenabled {
		_g_.racectx, raceprocctx0 = raceinit()
	}

	// 在调度器初始函数执行的过程中会将 maxmcount 设置成 10000，这也就是一个 Go 语言程序能够创建的最大线程数，
	// 虽然最多可以创建 10000 个线程，但是可以同时运行的线程还是由 GOMAXPROCS 变量控制。
	sched.maxmcount = 10000

	tracebackinit()
	moduledataverify()
	stackinit()
	mallocinit()
	fastrandinit() // must run before mcommoninit
	mcommoninit(_g_.m)
	cpuinit()       // must run before alginit
	alginit()       // maps must not be used before this call
	modulesinit()   // provides activeModules
	typelinksinit() // uses maps, activeModules
	itabsinit()     // uses activeModules

	msigsave(_g_.m)
	initSigmask = _g_.m.sigmask

	goargs()
	goenvs()
	parsedebugvars()
	// GC初始化
	gcinit()

	sched.lastpoll = uint64(nanotime())
	procs := ncpu
	if n, ok := atoi32(gogetenv("GOMAXPROCS")); ok && n > 0 {
		procs = n
	}
	// 从环境变量 GOMAXPROCS 获取了程序能够同时运行的最大处理器数之后就会调用 runtime.procresize 更新程序中处理器的数量
	// 在这时整个程序不会执行任何用户 Goroutine，调度器也会进入锁定状态
	if procresize(procs) != nil {
		throw("unknown runnable goroutine during bootstrap")
	}

	// For cgocheck > 1, we turn on the write barrier at all times
	// and check all pointer writes. We can't do this until after
	// procresize because the write barrier needs a P.
	if debug.cgocheck > 1 {
		writeBarrier.cgo = true
		writeBarrier.enabled = true
		for _, p := range allp {
			p.wbBuf.reset()
		}
	}

	if buildVersion == "" {
		// Condition should never trigger. This code just serves
		// to ensure runtime·buildVersion is kept in the resulting binary.
		buildVersion = "unknown"
	}
	if len(modinfo) == 1 {
		// Condition should never trigger. This code just serves
		// to ensure runtime·modinfo is kept in the resulting binary.
		modinfo = ""
	}
}

func dumpgstatus(gp *g) {
	_g_ := getg()
	print("runtime: gp: gp=", gp, ", goid=", gp.goid, ", gp->atomicstatus=", readgstatus(gp), "\n")
	print("runtime:  g:  g=", _g_, ", goid=", _g_.goid, ",  g->atomicstatus=", readgstatus(_g_), "\n")
}

func checkmcount() {
	// sched lock is held
	if mcount() > sched.maxmcount {
		print("runtime: program exceeds ", sched.maxmcount, "-thread limit\n")
		throw("thread exhaustion")
	}
}

func mcommoninit(mp *m) {
	_g_ := getg()

	// g0 stack won't make sense for user (and is not necessary unwindable).
	if _g_ != _g_.m.g0 {
		callers(1, mp.createstack[:])
	}

	lock(&sched.lock)
	if sched.mnext+1 < sched.mnext {
		throw("runtime: thread ID overflow")
	}
	mp.id = sched.mnext
	sched.mnext++
	checkmcount()

	mp.fastrand[0] = uint32(int64Hash(uint64(mp.id), fastrandseed))
	mp.fastrand[1] = uint32(int64Hash(uint64(cputicks()), ^fastrandseed))
	if mp.fastrand[0]|mp.fastrand[1] == 0 {
		mp.fastrand[1] = 1
	}

	mpreinit(mp)
	if mp.gsignal != nil {
		mp.gsignal.stackguard1 = mp.gsignal.stack.lo + _StackGuard
	}

	// Add to allm so garbage collector doesn't free g->m
	// when it is just in a register or thread-local storage.
	mp.alllink = allm

	// NumCgoCall() iterates over allm w/o schedlock,
	// so we need to publish it safely.
	atomicstorep(unsafe.Pointer(&allm), unsafe.Pointer(mp))
	unlock(&sched.lock)

	// Allocate memory to hold a cgo traceback if the cgo call crashes.
	if iscgo || GOOS == "solaris" || GOOS == "illumos" || GOOS == "windows" {
		mp.cgoCallers = new(cgoCallers)
	}
}

var fastrandseed uintptr

func fastrandinit() {
	s := (*[unsafe.Sizeof(fastrandseed)]byte)(unsafe.Pointer(&fastrandseed))[:]
	getRandomData(s)
}

// Mark gp ready to run.
func ready(gp *g, traceskip int, next bool) {
	if trace.enabled {
		traceGoUnpark(gp, traceskip)
	}

	status := readgstatus(gp)

	// Mark runnable.
	_g_ := getg()
	mp := acquirem() // disable preemption because it can be holding p in a local var
	if status&^_Gscan != _Gwaiting {
		dumpgstatus(gp)
		throw("bad g->status in ready")
	}

	// status is Gwaiting or Gscanwaiting, make Grunnable and put on runq
	casgstatus(gp, _Gwaiting, _Grunnable)
	runqput(_g_.m.p.ptr(), gp, next)
	wakep()
	releasem(mp)
}

// freezeStopWait is a large value that freezetheworld sets
// sched.stopwait to in order to request that all Gs permanently stop.
const freezeStopWait = 0x7fffffff

// freezing is set to non-zero if the runtime is trying to freeze the
// world.
var freezing uint32

// Similar to stopTheWorld but best-effort and can be called several times.
// There is no reverse operation, used during crashing.
// This function must not lock any mutexes.
func freezetheworld() {
	atomic.Store(&freezing, 1)
	// stopwait and preemption requests can be lost
	// due to races with concurrently executing threads,
	// so try several times
	for i := 0; i < 5; i++ {
		// this should tell the scheduler to not start any new goroutines
		sched.stopwait = freezeStopWait
		atomic.Store(&sched.gcwaiting, 1)
		// this should stop running goroutines
		if !preemptall() {
			break // no running goroutines
		}
		usleep(1000)
	}
	// to be sure
	usleep(1000)
	preemptall()
	usleep(1000)
}

// All reads and writes of g's status go through readgstatus, casgstatus
// castogscanstatus, casfrom_Gscanstatus.
//go:nosplit
func readgstatus(gp *g) uint32 {
	return atomic.Load(&gp.atomicstatus)
}

// The Gscanstatuses are acting like locks and this releases them.
// If it proves to be a performance hit we should be able to make these
// simple atomic stores but for now we are going to throw if
// we see an inconsistent state.
func casfrom_Gscanstatus(gp *g, oldval, newval uint32) {
	success := false

	// Check that transition is valid.
	switch oldval {
	default:
		print("runtime: casfrom_Gscanstatus bad oldval gp=", gp, ", oldval=", hex(oldval), ", newval=", hex(newval), "\n")
		dumpgstatus(gp)
		throw("casfrom_Gscanstatus:top gp->status is not in scan state")
	case _Gscanrunnable,
		_Gscanwaiting,
		_Gscanrunning,
		_Gscansyscall,
		_Gscanpreempted:
		if newval == oldval&^_Gscan {
			success = atomic.Cas(&gp.atomicstatus, oldval, newval)
		}
	}
	if !success {
		print("runtime: casfrom_Gscanstatus failed gp=", gp, ", oldval=", hex(oldval), ", newval=", hex(newval), "\n")
		dumpgstatus(gp)
		throw("casfrom_Gscanstatus: gp->status is not in scan state")
	}
	releaseLockRank(lockRankGscan)
}

// This will return false if the gp is not in the expected status and the cas fails.
// This acts like a lock acquire while the casfromgstatus acts like a lock release.
func castogscanstatus(gp *g, oldval, newval uint32) bool {
	switch oldval {
	case _Grunnable,
		_Grunning,
		_Gwaiting,
		_Gsyscall:
		if newval == oldval|_Gscan {
			r := atomic.Cas(&gp.atomicstatus, oldval, newval)
			if r {
				acquireLockRank(lockRankGscan)
			}
			return r

		}
	}
	print("runtime: castogscanstatus oldval=", hex(oldval), " newval=", hex(newval), "\n")
	throw("castogscanstatus")
	panic("not reached")
}

// If asked to move to or from a Gscanstatus this will throw. Use the castogscanstatus
// and casfrom_Gscanstatus instead.
// casgstatus will loop if the g->atomicstatus is in a Gscan status until the routine that
// put it in the Gscan state is finished.
//go:nosplit
func casgstatus(gp *g, oldval, newval uint32) {
	if (oldval&_Gscan != 0) || (newval&_Gscan != 0) || oldval == newval {
		systemstack(func() {
			print("runtime: casgstatus: oldval=", hex(oldval), " newval=", hex(newval), "\n")
			throw("casgstatus: bad incoming values")
		})
	}

	acquireLockRank(lockRankGscan)
	releaseLockRank(lockRankGscan)

	// See https://golang.org/cl/21503 for justification of the yield delay.
	const yieldDelay = 5 * 1000
	var nextYield int64

	// loop if gp->atomicstatus is in a scan state giving
	// GC time to finish and change the state to oldval.
	for i := 0; !atomic.Cas(&gp.atomicstatus, oldval, newval); i++ {
		if oldval == _Gwaiting && gp.atomicstatus == _Grunnable {
			throw("casgstatus: waiting for Gwaiting but is Grunnable")
		}
		if i == 0 {
			nextYield = nanotime() + yieldDelay
		}
		if nanotime() < nextYield {
			for x := 0; x < 10 && gp.atomicstatus != oldval; x++ {
				procyield(1)
			}
		} else {
			osyield()
			nextYield = nanotime() + yieldDelay/2
		}
	}
}

// casgstatus(gp, oldstatus, Gcopystack), assuming oldstatus is Gwaiting or Grunnable.
// Returns old status. Cannot call casgstatus directly, because we are racing with an
// async wakeup that might come in from netpoll. If we see Gwaiting from the readgstatus,
// it might have become Grunnable by the time we get to the cas. If we called casgstatus,
// it would loop waiting for the status to go back to Gwaiting, which it never will.
//go:nosplit
func casgcopystack(gp *g) uint32 {
	for {
		oldstatus := readgstatus(gp) &^ _Gscan
		if oldstatus != _Gwaiting && oldstatus != _Grunnable {
			throw("copystack: bad status, not Gwaiting or Grunnable")
		}
		if atomic.Cas(&gp.atomicstatus, oldstatus, _Gcopystack) {
			return oldstatus
		}
	}
}

// casGToPreemptScan transitions gp from _Grunning to _Gscan|_Gpreempted.
//
// TODO(austin): This is the only status operation that both changes
// the status and locks the _Gscan bit. Rethink this.
func casGToPreemptScan(gp *g, old, new uint32) {
	if old != _Grunning || new != _Gscan|_Gpreempted {
		throw("bad g transition")
	}
	acquireLockRank(lockRankGscan)
	for !atomic.Cas(&gp.atomicstatus, _Grunning, _Gscan|_Gpreempted) {
	}
}

// casGFromPreempted attempts to transition gp from _Gpreempted to
// _Gwaiting. If successful, the caller is responsible for
// re-scheduling gp.
func casGFromPreempted(gp *g, old, new uint32) bool {
	if old != _Gpreempted || new != _Gwaiting {
		throw("bad g transition")
	}
	return atomic.Cas(&gp.atomicstatus, _Gpreempted, _Gwaiting)
}

// stopTheWorld stops all P's from executing goroutines, interrupting
// all goroutines at GC safe points and records reason as the reason
// for the stop. On return, only the current goroutine's P is running.
// stopTheWorld must not be called from a system stack and the caller
// must not hold worldsema. The caller must call startTheWorld when
// other P's should resume execution.
//
// stopTheWorld is safe for multiple goroutines to call at the
// same time. Each will execute its own stop, and the stops will
// be serialized.
//
// This is also used by routines that do stack dumps. If the system is
// in panic or being exited, this may not reliably stop all
// goroutines.
func stopTheWorld(reason string) {
	semacquire(&worldsema)
	gp := getg()
	gp.m.preemptoff = reason
	systemstack(func() {
		// Mark the goroutine which called stopTheWorld preemptible so its
		// stack may be scanned.
		// This lets a mark worker scan us while we try to stop the world
		// since otherwise we could get in a mutual preemption deadlock.
		// We must not modify anything on the G stack because a stack shrink
		// may occur. A stack shrink is otherwise OK though because in order
		// to return from this function (and to leave the system stack) we
		// must have preempted all goroutines, including any attempting
		// to scan our stack, in which case, any stack shrinking will
		// have already completed by the time we exit.
		casgstatus(gp, _Grunning, _Gwaiting)
		stopTheWorldWithSema()
		casgstatus(gp, _Gwaiting, _Grunning)
	})
}

// startTheWorld undoes the effects of stopTheWorld.
func startTheWorld() {
	systemstack(func() { startTheWorldWithSema(false) })
	// worldsema must be held over startTheWorldWithSema to ensure
	// gomaxprocs cannot change while worldsema is held.
	semrelease(&worldsema)
	getg().m.preemptoff = ""
}

// stopTheWorldGC has the same effect as stopTheWorld, but blocks
// until the GC is not running. It also blocks a GC from starting
// until startTheWorldGC is called.
func stopTheWorldGC(reason string) {
	semacquire(&gcsema)
	stopTheWorld(reason)
}

// startTheWorldGC undoes the effects of stopTheWorldGC.
func startTheWorldGC() {
	startTheWorld()
	semrelease(&gcsema)
}

// Holding worldsema grants an M the right to try to stop the world.
// 是全局的信号量，获取该信号量的线程有权利暂停当前应用程序；
var worldsema uint32 = 1

// Holding gcsema grants the M the right to block a GC, and blocks
// until the current GC is done. In particular, it prevents gomaxprocs
// from changing concurrently.
//
// TODO(mknyszek): Once gomaxprocs and the execution tracer can handle
// being changed/enabled during a GC, remove this.
var gcsema uint32 = 1

// stopTheWorldWithSema is the core implementation of stopTheWorld.
// The caller is responsible for acquiring worldsema and disabling
// preemption first and then should stopTheWorldWithSema on the system
// stack:
//
//	semacquire(&worldsema, 0)
//	m.preemptoff = "reason"
//	systemstack(stopTheWorldWithSema)
//
// When finished, the caller must either call startTheWorld or undo
// these three operations separately:
//
//	m.preemptoff = ""
//	systemstack(startTheWorldWithSema)
//	semrelease(&worldsema)
//
// It is allowed to acquire worldsema once and then execute multiple
// startTheWorldWithSema/stopTheWorldWithSema pairs.
// Other P's are able to execute between successive calls to
// startTheWorldWithSema and stopTheWorldWithSema.
// Holding worldsema causes any other goroutines invoking
// stopTheWorld to block.

// STW 要确保所有被调度器调度执行的用户代码停止执行，
// 一个可行的方案就是让所有的 M 与其关联的 P 解除绑定， 这样 M 便无法再继续执行用户代码了。
// STW开始的逻辑
// 1.尝试为每个处于运行状态 (_Prunning) 的 P 上的 goroutine 设置抢占标记，这样所有正在运行的 G 在发生进一步函数调用时，会主动被抢占
// 2.抢占所有正在进行系统调用状态（_Psyscall）时还来不及被抢夺的 P
// 3.抢占所有处于空闲状态尚未被 M 绑定的 P
// 4.等待所有的P都与M成功解绑
func stopTheWorldWithSema() {
	_g_ := getg()

	// If we hold a lock, then we won't be able to stop another M
	// that is blocked trying to acquire the lock.
	if _g_.m.locks > 0 {
		throw("stopTheWorld: holding locks")
	}

	lock(&sched.lock)
	// 停止调度器需要停止的线程数
	sched.stopwait = gomaxprocs
	// 设置因 GC 需要停止调度器的标志位
	atomic.Store(&sched.gcwaiting, 1)
	// 抢占所有当前执行的 G
	preemptall()
	// stop current P
	// 停止当前 G 所在的的 P，这时需要解绑的数量少了一个
	_g_.m.p.ptr().status = _Pgcstop // Pgcstop is only diagnostic.
	sched.stopwait--
	// try to retake all P's in Psyscall status
	// 抢占所有在系统调用 Psyscall 状态的 P
	for _, p := range allp {
		s := p.status
		if s == _Psyscall && atomic.Cas(&p.status, s, _Pgcstop) {
			if trace.enabled {
				traceGoSysBlock(p)
				traceProcStop(p)
			}
			p.syscalltick++
			sched.stopwait--
		}
	}
	// stop idle P's
	// 抢占所有空闲的 P，防止再次被抢走
	for {
		p := pidleget()
		if p == nil {
			break
		}
		p.status = _Pgcstop
		sched.stopwait--
	}
	wait := sched.stopwait > 0
	unlock(&sched.lock)

	// wait for remaining P's to stop voluntarily
	// 等待剩余的无法被抢占的 P 主动停止
	if wait {
		for {
			// wait for 100us, then try to re-preempt in case of any races
			// 等待 100 微秒，然后尝试重新抢占，从而防止竞争
			if notetsleep(&sched.stopnote, 100*1000) {
				noteclear(&sched.stopnote)
				break
			}
			preemptall()
		}
	}

	// sanity checks
	bad := ""
	if sched.stopwait != 0 {
		bad = "stopTheWorld: not stopped (stopwait != 0)"
	} else {
		for _, p := range allp {
			if p.status != _Pgcstop {
				bad = "stopTheWorld: not stopped (status != _Pgcstop)"
			}
		}
	}
	if atomic.Load(&freezing) != 0 {
		// Some other thread is panicking. This can cause the
		// sanity checks above to fail if the panic happens in
		// the signal handler on a stopped thread. Either way,
		// we should halt this thread.
		lock(&deadlock)
		lock(&deadlock)
	}
	if bad != "" {
		throw(bad)
	}
}

// 当要结束STW阶段时，无非就是唤醒调度器，即唤醒已经处于休眠状态的M，重新开始调度G
// STW结束的逻辑：
// 1.网络数据优先级最高，优先确定需要处理的网络数据
// 2.其次唤醒系统监控
// 3.最后再依次唤醒 M，将其绑定 P，并重新开始调度 G
func startTheWorldWithSema(emitTraceEvent bool) int64 {
	mp := acquirem() // disable preemption because it can be holding p in a local var
	if netpollinited() {
		list := netpoll(0) // non-blocking
		injectglist(&list)
	}
	lock(&sched.lock)

	// 处理器 P 的数量调整
	procs := gomaxprocs
	if newprocs != 0 {
		procs = newprocs
		newprocs = 0
	}
	p1 := procresize(procs)

	// 调度器可以开始调度了
	sched.gcwaiting = 0
	// 唤醒系统监控
	if sched.sysmonwait != 0 {
		sched.sysmonwait = 0
		notewakeup(&sched.sysmonnote)
	}
	unlock(&sched.lock)

	// 依次将 m 唤醒，并绑定到 p，开始执行
	for p1 != nil {
		p := p1
		p1 = p1.link.ptr()
		if p.m != 0 {
			mp := p.m.ptr()
			p.m = 0
			if mp.nextp != 0 {
				throw("startTheWorld: inconsistent mp->nextp")
			}
			// 将m绑定到p
			mp.nextp.set(p)
			// 将m唤醒
			notewakeup(&mp.park)
		} else {
			// Start M to run P.  Do not start another M below.
			// 运行 M 并运行 P. 下面不会创建一个新的 M
			newm(nil, p)
		}
	}

	// Capture start-the-world time before doing clean-up tasks.
	startTime := nanotime()
	if emitTraceEvent {
		traceGCSTWDone()
	}

	// Wakeup an additional proc in case we have excessive runnable goroutines
	// in local queues or in the global queue. If we don't, the proc will park itself.
	// If we have lots of excessive work, resetspinning will unpark additional procs as necessary.
	// 如果我们在本地队列或全局队列中有过多的可运行的 goroutine，则唤醒一个额外的 proc。
	// 如果我们不这样做，那么过程就会停止。
	// 如果我们有大量过多的工作，重新设置将取消必要的额外过程。
	wakep()

	releasem(mp)

	return startTime
}

/*
mstart 是 Go 运行时中的一个函数，用于新建 M（Machine）的入口点。
M 是 Go 运行时中的一个抽象概念，表示一个操作系统线程。
Go 运行时使用 M 来管理和调度 Goroutine 的执行。
mstart 函数是用汇编语言编写的，使用 ABI0 调用约定，并且标记为 TOPFRAME，表示它是调用栈的顶层框架。
mstart 函数最终会调用 mstart0 函数来完成 M 的初始化和启动。
*/
// mstart is the entry-point for new Ms.
//
// This must not split the stack because we may not even have stack
// bounds set up yet.
//
// May run during STW (because it doesn't have a P yet), so write
// barriers are not allowed.
//
//go:nosplit
//go:nowritebarrierrec
func mstart() {
	// 获取当前执行的 Goroutine 的指针
	// 这个指针指向一个 g 结构体，g 结构体包含了当前 Goroutine 的所有状态信息，包括栈指针、程序计数器、调度器状态等。
	_g_ := getg()

	// 判断当前执行的goroutine是否是g0
	// g0 是唯一一个会在系统栈上执行的 Goroutine。
	osStack := _g_.stack.lo == 0
	if osStack {
		// Initialize stack bounds from system stack.
		// Cgo may have left stack size in stack.hi.
		// minit may update the stack bounds.
		size := _g_.stack.hi
		// 如果 stack.hi 的值为 0，表示栈的大小尚未初始化。此时，代码会将 size 设置为一个默认值。
		if size == 0 {
			size = 8192 * sys.StackGuardMultiplier
		}
		// 将 size 作为栈的大小，设置到 g 结构体的 stack.hi 字段中。
		// noescape：这是一个编译器内建函数，表示这个指针不会在函数返回后逃逸。
		// 这样可以避免垃圾回收器错误地认为这个指针在函数返回后仍然有效。
		_g_.stack.hi = uintptr(noescape(unsafe.Pointer(&size)))
		// 重新计算栈的低地址。减去1024是为了防止栈溢出。
		_g_.stack.lo = _g_.stack.hi - size + 1024
	}

	/*
		栈保护：
		栈保护是一个用于检测栈溢出的机制。Go 运行时系统使用两个栈保护变量 stackguard0 和 stackguard1 来实现这一机制：
		1.stackguard0：用于常规的 Go 代码调用。它是一个阈值，当栈指针（stack pointer）接近这个阈值时，Go 运行时系统会触发栈扩展或其他保护措施。
		2.stackguard1：用于 go:systemstack 标记的函数调用。这些函数通常是运行时系统的一部分，需要在系统栈上执行。
	*/

	// Initialize stack guard so that we can start calling regular
	// Go code.
	// 这行代码将 stackguard0 设置为栈底（stack.lo）加上一个常量 _StackGuard。
	// 这个常量 _StackGuard 是一个预定义的偏移量，用于确保在栈溢出之前有足够的空间进行检测和处理。
	_g_.stackguard0 = _g_.stack.lo + _StackGuard
	// This is the g0, so we can also call go:systemstack
	// functions, which check stackguard1.
	// 由于这是 g0 Goroutine，它可以调用 go:systemstack 标记的函数，这些函数会检查 stackguard1。
	// 因此，将 stackguard1 设置为与 stackguard0 相同的值。
	_g_.stackguard1 = _g_.stackguard0
	mstart1()

	// Exit this thread.
	// 用于在特定操作系统上退出当前线程。
	// 它首先检查当前操作系统类型，并根据操作系统类型设置 osStack 变量，然后调用 mexit 函数退出线程。
	switch GOOS {
	// GOOS 是一个预定义的编译时常量，表示目标操作系统
	case "windows", "solaris", "illumos", "plan9", "darwin", "aix":
		// Windows, Solaris, illumos, Darwin, AIX and Plan 9 always system-allocate
		// the stack, but put it in _g_.stack before mstart,
		// so the logic above hasn't set osStack yet.
		osStack = true
	}
	// mexit 函数是 Go 运行时系统中的一个关键函数，用于退出当前线程。
	mexit(osStack)
}

/*
	mstart1 是 Go 运行时系统中的一个关键函数，用于初始化和启动一个新的 M（操作系统线程）。这个函数在 mstart 函数中被调用，负责设置线程的初始状态，并进入调度循环。

*/
func mstart1() {
	_g_ := getg()

	// g0 是一个特殊的 Goroutine，负责执行调度器和一些运行时的管理任务。
	if _g_ != _g_.m.g0 {
		throw("bad runtime·mstart")
	}

	// Record the caller for use as the top of stack in mcall and
	// for terminating the thread.
	// We're never coming back to mstart1 after we call schedule,
	// so other calls can reuse the current frame.
	// save 函数记录调用者的程序计数器（PC）和栈指针（SP），以便在 mcall 和终止线程时使用。
	save(getcallerpc(), getcallersp())
	// 初始化汇编相关内容
	asminit()
	// 进行 M 的初始化，包括设置线程本地存储（TLS）和信号掩码。
	minit()

	// Install signal handlers; after minit so that minit can
	// prepare the thread to be able to handle the signals.
	// 如果当前 M 是 m0，则调用 mstartm0 函数安装信号处理程序。
	// m0 是 Go 运行时系统启动时创建的第一个 M。
	// 如果当前 M 是主线程（m0），则调用 mstartm0 安装信号处理程序。信号处理程序的安装在 minit 之后进行，以确保线程已准备好处理信号。
	if _g_.m == &m0 {
		mstartm0()
	}
	// 如果 mstartfn 不为 nil，则调用它。mstartfn 是一个可选的函数指针，用于在 M 启动时执行一些自定义初始化代码。
	if fn := _g_.m.mstartfn; fn != nil {
		fn()
	}
	// 如果当前 M 不是 m0，则获取下一个 P（处理器），并将 nextp 设置为 0。【因为m0已经在入口的汇编代码中，绑了一个p】
	// P 是 Go 运行时系统中的一个抽象概念，表示一个逻辑处理器。
	if _g_.m != &m0 {
		acquirep(_g_.m.nextp.ptr())
		_g_.m.nextp = 0
	}
	// 调用 schedule 函数进入调度循环，开始调度和执行 Goroutine。
	/*
		这里调用schedule的本质是，开始获取G并执行G。
		但当前的M是m0，m0绑定的p中，有 runtime.main 对应的G，
		这个G是在Go程序启动时执行的汇编文件【例如：asm_amd64s】中，放进去的，所以最终会执行runtime.main
	*/
	schedule()
}

// mstartm0 implements part of mstart1 that only runs on the m0.
//
// Write barriers are allowed here because we know the GC can't be
// running yet, so they'll be no-ops.
//
//go:yeswritebarrierrec
func mstartm0() {
	// Create an extra M for callbacks on threads not created by Go.
	// An extra M is also needed on Windows for callbacks created by
	// syscall.NewCallback. See issue #6751 for details.
	if (iscgo || GOOS == "windows") && !cgoHasExtraM {
		cgoHasExtraM = true
		newextram()
	}
	initsig(false)
}

// mexit tears down and exits the current thread.
//
// Don't call this directly to exit the thread, since it must run at
// the top of the thread stack. Instead, use gogo(&_g_.m.g0.sched) to
// unwind the stack to the point that exits the thread.
//
// It is entered with m.p != nil, so write barriers are allowed. It
// will release the P before exiting.
//
//go:yeswritebarrierrec
func mexit(osStack bool) {
	g := getg()
	m := g.m

	if m == &m0 {
		// This is the main thread. Just wedge it.
		//
		// On Linux, exiting the main thread puts the process
		// into a non-waitable zombie state. On Plan 9,
		// exiting the main thread unblocks wait even though
		// other threads are still running. On Solaris we can
		// neither exitThread nor return from mstart. Other
		// bad things probably happen on other platforms.
		//
		// We could try to clean up this M more before wedging
		// it, but that complicates signal handling.
		// 释放当前 M 持有的 P。
		handoffp(releasep())
		// 锁定调度器
		lock(&sched.lock)
		// 增加已释放的 M 的计数。
		sched.nmfreed++
		// 调用死锁检测器，检查是否存在死锁情况。
		checkdead()
		// 解锁调度器
		unlock(&sched.lock)
		// 将主线程置于休眠状态，等待被唤醒。
		notesleep(&m.park)
		// 如果主线程被意外唤醒，则抛出异常。这是一种防御性编程措施，确保主线程在不应该被唤醒时不会继续执行。
		throw("locked m0 woke up")
	}

	// 阻塞信号处理
	// 这是为了确保在退出线程的过程中不会被信号中断，从而避免潜在的竞态条件和不一致状态。
	sigblock()
	// 取消 M 的初始化
	// 调用 unminit 函数取消当前 M 的初始化状态。
	// 这通常包括清理线程本地存储（TLS）等资源。
	// unminit 函数的具体实现可能因平台而异，但其目的是确保在退出线程之前清理与 M 相关的所有资源。
	unminit()

	// Free the gsignal stack.
	// 检查 gsignal 栈：如果当前 M 有一个 gsignal 栈（用于处理信号的栈），则执行以下操作。
	// 1. 释放 gsignal 栈：调用 stackfree 函数释放 gsignal 栈的内存。
	// 2. 解除 gsignal 栈的链接：将 m.gsignal 设置为 nil。
	// 在某些平台上，当调用 VDSO（如 nanotime）时，会将当前的 Goroutine 存储在 gsignal 栈上。
	// 现在栈已经被释放，因此需要解除它与 M 的链接，以避免在调用 VDSO 代码时写入已释放的栈。
	if m.gsignal != nil {
		stackfree(m.gsignal.stack)
		// On some platforms, when calling into VDSO (e.g. nanotime)
		// we store our g on the gsignal stack, if there is one.
		// Now the stack is freed, unlink it from the m, so we
		// won't write to it when calling VDSO code.
		m.gsignal = nil
	}

	// Remove m from allm.
	// 这段代码的目的是将当前的 M（线程）从全局 M 列表 allm 中移除。allm 是一个链表，包含了所有已创建的 M。让我们逐步解析这段代码。
	// 这行代码锁定了调度器的全局锁 sched.lock。这样可以确保在操作 allm 列表时不会有其他线程同时修改它，从而避免竞态条件。
	lock(&sched.lock)
	for pprev := &allm; *pprev != nil; pprev = &(*pprev).alllink {
		// 在遍历过程中，检查当前节点是否是要移除的 M。
		// 如果是，则将 pprev 指向 m.alllink，即将当前节点从链表中移除。然后跳转到 found 标签。
		if *pprev == m {
			*pprev = m.alllink
			goto found
		}
	}
	// 如果遍历完整个 allm 列表后仍未找到要移除的 M，则抛出异常 "m not found in allm"。
	// 这是一种防御性编程措施，用于捕捉不应该发生的错误情况。
	throw("m not found in allm")
found:
	// 这个标签表示已经成功找到并移除了 M。后续代码会继续执行。
	if !osStack {
		// Delay reaping m until it's done with the stack.
		//
		// If this is using an OS stack, the OS will free it
		// so there's no need for reaping.
		// 设置 freeWait 标志：使用原子操作将 m.freeWait 设置为 1，表示当前 M 还不能被回收。
		atomic.Store(&m.freeWait, 1)
		// Put m on the free list, though it will not be reaped until
		// freeWait is 0. Note that the free list must not be linked
		// through alllink because some functions walk allm without
		// locking, so may be using alllink.
		// 将 M 放入空闲列表：将当前 M 放入调度器的空闲列表 sched.freem 中。
		// 注意，这里使用 freelink 而不是 alllink，因为有些函数在遍历 allm 时不加锁。
		m.freelink = sched.freem
		sched.freem = m
	}
	// 解锁调度器的全局锁 sched.lock，允许其他线程继续操作调度器。
	unlock(&sched.lock)

	// Release the P.
	// 释放 P：调用 handoffp(releasep()) 释放当前 M 持有的 P。
	// 禁止写屏障：在这之后不应再使用写屏障，因为当前 M 可能会被回收。
	handoffp(releasep())
	// After this point we must not have write barriers.

	// Invoke the deadlock detector. This must happen after
	// handoffp because it may have started a new M to take our
	// P's work.
	// 锁定调度器：再次锁定调度器的全局锁 sched.lock。
	lock(&sched.lock)
	// 更新已释放的 M 计数：增加 sched.nmfreed 计数。
	sched.nmfreed++
	// 调用死锁检测器：调用 checkdead() 检查是否存在死锁情况。
	checkdead()
	// 解锁调度器：解锁调度器的全局锁 sched.lock。
	unlock(&sched.lock)

	if osStack {
		// Return from mstart and let the system thread
		// library free the g0 stack and terminate the thread.
		// 如果当前线程使用的是操作系统栈，则直接从 mstart 返回，让系统线程库释放 g0 栈并终止线程。
		return
	}

	// mstart is the thread's entry point, so there's nothing to
	// return to. Exit the thread directly. exitThread will clear
	// m.freeWait when it's done with the stack and the m can be
	// reaped.
	// 直接退出线程：调用 exitThread(&m.freeWait) 直接退出线程。
	// exitThread 会在完成栈的处理后清除 m.freeWait，这样 M 就可以被回收。
	exitThread(&m.freeWait)
}

// forEachP calls fn(p) for every P p when p reaches a GC safe point.
// If a P is currently executing code, this will bring the P to a GC
// safe point and execute fn on that P. If the P is not executing code
// (it is idle or in a syscall), this will call fn(p) directly while
// preventing the P from exiting its state. This does not ensure that
// fn will run on every CPU executing Go code, but it acts as a global
// memory barrier. GC uses this as a "ragged barrier."
//
// The caller must hold worldsema.
//
//go:systemstack
func forEachP(fn func(*p)) {
	mp := acquirem()
	_p_ := getg().m.p.ptr()

	lock(&sched.lock)
	if sched.safePointWait != 0 {
		throw("forEachP: sched.safePointWait != 0")
	}
	sched.safePointWait = gomaxprocs - 1
	sched.safePointFn = fn

	// Ask all Ps to run the safe point function.
	for _, p := range allp {
		if p != _p_ {
			atomic.Store(&p.runSafePointFn, 1)
		}
	}
	preemptall()

	// Any P entering _Pidle or _Psyscall from now on will observe
	// p.runSafePointFn == 1 and will call runSafePointFn when
	// changing its status to _Pidle/_Psyscall.

	// Run safe point function for all idle Ps. sched.pidle will
	// not change because we hold sched.lock.
	for p := sched.pidle.ptr(); p != nil; p = p.link.ptr() {
		if atomic.Cas(&p.runSafePointFn, 1, 0) {
			fn(p)
			sched.safePointWait--
		}
	}

	wait := sched.safePointWait > 0
	unlock(&sched.lock)

	// Run fn for the current P.
	fn(_p_)

	// Force Ps currently in _Psyscall into _Pidle and hand them
	// off to induce safe point function execution.
	for _, p := range allp {
		s := p.status
		if s == _Psyscall && p.runSafePointFn == 1 && atomic.Cas(&p.status, s, _Pidle) {
			if trace.enabled {
				traceGoSysBlock(p)
				traceProcStop(p)
			}
			p.syscalltick++
			handoffp(p)
		}
	}

	// Wait for remaining Ps to run fn.
	// 等待剩余的
	if wait {
		for {
			// Wait for 100us, then try to re-preempt in
			// case of any races.
			//
			// Requires system stack.
			if notetsleep(&sched.safePointNote, 100*1000) {
				noteclear(&sched.safePointNote)
				break
			}
			preemptall()
		}
	}
	if sched.safePointWait != 0 {
		throw("forEachP: not done")
	}
	for _, p := range allp {
		if p.runSafePointFn != 0 {
			throw("forEachP: P did not run fn")
		}
	}

	lock(&sched.lock)
	sched.safePointFn = nil
	unlock(&sched.lock)
	releasem(mp)
}

// runSafePointFn runs the safe point function, if any, for this P.
// This should be called like
//
//     if getg().m.p.runSafePointFn != 0 {
//         runSafePointFn()
//     }
//
// runSafePointFn must be checked on any transition in to _Pidle or
// _Psyscall to avoid a race where forEachP sees that the P is running
// just before the P goes into _Pidle/_Psyscall and neither forEachP
// nor the P run the safe-point function.
func runSafePointFn() {
	p := getg().m.p.ptr()
	// Resolve the race between forEachP running the safe-point
	// function on this P's behalf and this P running the
	// safe-point function directly.
	if !atomic.Cas(&p.runSafePointFn, 1, 0) {
		return
	}
	sched.safePointFn(p)
	lock(&sched.lock)
	sched.safePointWait--
	if sched.safePointWait == 0 {
		notewakeup(&sched.safePointNote)
	}
	unlock(&sched.lock)
}

// When running with cgo, we call _cgo_thread_start
// to start threads for us so that we can play nicely with
// foreign code.
var cgoThreadStart unsafe.Pointer

type cgothreadstart struct {
	g   guintptr
	tls *uint64
	fn  unsafe.Pointer
}

// Allocate a new m unassociated with any thread.
// Can use p for allocation context if needed.
// fn is recorded as the new m's m.mstartfn.
//
// This function is allowed to have write barriers even if the caller
// isn't because it borrows _p_.
//
//go:yeswritebarrierrec
func allocm(_p_ *p, fn func()) *m {
	_g_ := getg()
	acquirem() // disable GC because it can be called from sysmon
	if _g_.m.p == 0 {
		acquirep(_p_) // temporarily borrow p for mallocs in this function
	}

	// Release the free M list. We need to do this somewhere and
	// this may free up a stack we can use.
	if sched.freem != nil {
		lock(&sched.lock)
		var newList *m
		for freem := sched.freem; freem != nil; {
			if freem.freeWait != 0 {
				next := freem.freelink
				freem.freelink = newList
				newList = freem
				freem = next
				continue
			}
			stackfree(freem.g0.stack)
			freem = freem.freelink
		}
		sched.freem = newList
		unlock(&sched.lock)
	}

	mp := new(m)
	mp.mstartfn = fn
	mcommoninit(mp)

	// In case of cgo or Solaris or illumos or Darwin, pthread_create will make us a stack.
	// Windows and Plan 9 will layout sched stack on OS stack.
	if iscgo || GOOS == "solaris" || GOOS == "illumos" || GOOS == "windows" || GOOS == "plan9" || GOOS == "darwin" {
		mp.g0 = malg(-1)
	} else {
		mp.g0 = malg(8192 * sys.StackGuardMultiplier)
	}
	mp.g0.m = mp

	if _p_ == _g_.m.p.ptr() {
		releasep()
	}
	releasem(_g_.m)

	return mp
}

// needm is called when a cgo callback happens on a
// thread without an m (a thread not created by Go).
// In this case, needm is expected to find an m to use
// and return with m, g initialized correctly.
// Since m and g are not set now (likely nil, but see below)
// needm is limited in what routines it can call. In particular
// it can only call nosplit functions (textflag 7) and cannot
// do any scheduling that requires an m.
//
// In order to avoid needing heavy lifting here, we adopt
// the following strategy: there is a stack of available m's
// that can be stolen. Using compare-and-swap
// to pop from the stack has ABA races, so we simulate
// a lock by doing an exchange (via Casuintptr) to steal the stack
// head and replace the top pointer with MLOCKED (1).
// This serves as a simple spin lock that we can use even
// without an m. The thread that locks the stack in this way
// unlocks the stack by storing a valid stack head pointer.
//
// In order to make sure that there is always an m structure
// available to be stolen, we maintain the invariant that there
// is always one more than needed. At the beginning of the
// program (if cgo is in use) the list is seeded with a single m.
// If needm finds that it has taken the last m off the list, its job
// is - once it has installed its own m so that it can do things like
// allocate memory - to create a spare m and put it on the list.
//
// Each of these extra m's also has a g0 and a curg that are
// pressed into service as the scheduling stack and current
// goroutine for the duration of the cgo callback.
//
// When the callback is done with the m, it calls dropm to
// put the m back on the list.
//go:nosplit
func needm(x byte) {
	if (iscgo || GOOS == "windows") && !cgoHasExtraM {
		// Can happen if C/C++ code calls Go from a global ctor.
		// Can also happen on Windows if a global ctor uses a
		// callback created by syscall.NewCallback. See issue #6751
		// for details.
		//
		// Can not throw, because scheduler is not initialized yet.
		write(2, unsafe.Pointer(&earlycgocallback[0]), int32(len(earlycgocallback)))
		exit(1)
	}

	// Lock extra list, take head, unlock popped list.
	// nilokay=false is safe here because of the invariant above,
	// that the extra list always contains or will soon contain
	// at least one m.
	mp := lockextra(false)

	// Set needextram when we've just emptied the list,
	// so that the eventual call into cgocallbackg will
	// allocate a new m for the extra list. We delay the
	// allocation until then so that it can be done
	// after exitsyscall makes sure it is okay to be
	// running at all (that is, there's no garbage collection
	// running right now).
	mp.needextram = mp.schedlink == 0
	extraMCount--
	unlockextra(mp.schedlink.ptr())

	// Save and block signals before installing g.
	// Once g is installed, any incoming signals will try to execute,
	// but we won't have the sigaltstack settings and other data
	// set up appropriately until the end of minit, which will
	// unblock the signals. This is the same dance as when
	// starting a new m to run Go code via newosproc.
	msigsave(mp)
	sigblock()

	// Install g (= m->g0) and set the stack bounds
	// to match the current stack. We don't actually know
	// how big the stack is, like we don't know how big any
	// scheduling stack is, but we assume there's at least 32 kB,
	// which is more than enough for us.
	setg(mp.g0)
	_g_ := getg()
	_g_.stack.hi = uintptr(noescape(unsafe.Pointer(&x))) + 1024
	_g_.stack.lo = uintptr(noescape(unsafe.Pointer(&x))) - 32*1024
	_g_.stackguard0 = _g_.stack.lo + _StackGuard

	// Initialize this thread to use the m.
	asminit()
	minit()

	// mp.curg is now a real goroutine.
	casgstatus(mp.curg, _Gdead, _Gsyscall)
	atomic.Xadd(&sched.ngsys, -1)
}

var earlycgocallback = []byte("fatal error: cgo callback before cgo call\n")

// newextram allocates m's and puts them on the extra list.
// It is called with a working local m, so that it can do things
// like call schedlock and allocate.
func newextram() {
	c := atomic.Xchg(&extraMWaiters, 0)
	if c > 0 {
		for i := uint32(0); i < c; i++ {
			oneNewExtraM()
		}
	} else {
		// Make sure there is at least one extra M.
		mp := lockextra(true)
		unlockextra(mp)
		if mp == nil {
			oneNewExtraM()
		}
	}
}

// oneNewExtraM allocates an m and puts it on the extra list.
func oneNewExtraM() {
	// Create extra goroutine locked to extra m.
	// The goroutine is the context in which the cgo callback will run.
	// The sched.pc will never be returned to, but setting it to
	// goexit makes clear to the traceback routines where
	// the goroutine stack ends.
	mp := allocm(nil, nil)
	gp := malg(4096)
	gp.sched.pc = funcPC(goexit) + sys.PCQuantum
	gp.sched.sp = gp.stack.hi
	gp.sched.sp -= 4 * sys.RegSize // extra space in case of reads slightly beyond frame
	gp.sched.lr = 0
	gp.sched.g = guintptr(unsafe.Pointer(gp))
	gp.syscallpc = gp.sched.pc
	gp.syscallsp = gp.sched.sp
	gp.stktopsp = gp.sched.sp
	// malg returns status as _Gidle. Change to _Gdead before
	// adding to allg where GC can see it. We use _Gdead to hide
	// this from tracebacks and stack scans since it isn't a
	// "real" goroutine until needm grabs it.
	casgstatus(gp, _Gidle, _Gdead)
	gp.m = mp
	mp.curg = gp
	mp.lockedInt++
	mp.lockedg.set(gp)
	gp.lockedm.set(mp)
	gp.goid = int64(atomic.Xadd64(&sched.goidgen, 1))
	if raceenabled {
		gp.racectx = racegostart(funcPC(newextram) + sys.PCQuantum)
	}
	// put on allg for garbage collector
	allgadd(gp)

	// gp is now on the allg list, but we don't want it to be
	// counted by gcount. It would be more "proper" to increment
	// sched.ngfree, but that requires locking. Incrementing ngsys
	// has the same effect.
	atomic.Xadd(&sched.ngsys, +1)

	// Add m to the extra list.
	mnext := lockextra(true)
	mp.schedlink.set(mnext)
	extraMCount++
	unlockextra(mp)
}

// dropm is called when a cgo callback has called needm but is now
// done with the callback and returning back into the non-Go thread.
// It puts the current m back onto the extra list.
//
// The main expense here is the call to signalstack to release the
// m's signal stack, and then the call to needm on the next callback
// from this thread. It is tempting to try to save the m for next time,
// which would eliminate both these costs, but there might not be
// a next time: the current thread (which Go does not control) might exit.
// If we saved the m for that thread, there would be an m leak each time
// such a thread exited. Instead, we acquire and release an m on each
// call. These should typically not be scheduling operations, just a few
// atomics, so the cost should be small.
//
// TODO(rsc): An alternative would be to allocate a dummy pthread per-thread
// variable using pthread_key_create. Unlike the pthread keys we already use
// on OS X, this dummy key would never be read by Go code. It would exist
// only so that we could register at thread-exit-time destructor.
// That destructor would put the m back onto the extra list.
// This is purely a performance optimization. The current version,
// in which dropm happens on each cgo call, is still correct too.
// We may have to keep the current version on systems with cgo
// but without pthreads, like Windows.
func dropm() {
	// Clear m and g, and return m to the extra list.
	// After the call to setg we can only call nosplit functions
	// with no pointer manipulation.
	mp := getg().m

	// Return mp.curg to dead state.
	casgstatus(mp.curg, _Gsyscall, _Gdead)
	mp.curg.preemptStop = false
	atomic.Xadd(&sched.ngsys, +1)

	// Block signals before unminit.
	// Unminit unregisters the signal handling stack (but needs g on some systems).
	// Setg(nil) clears g, which is the signal handler's cue not to run Go handlers.
	// It's important not to try to handle a signal between those two steps.
	sigmask := mp.sigmask
	sigblock()
	unminit()

	mnext := lockextra(true)
	extraMCount++
	mp.schedlink.set(mnext)

	setg(nil)

	// Commit the release of mp.
	unlockextra(mp)

	msigrestore(sigmask)
}

// A helper function for EnsureDropM.
func getm() uintptr {
	return uintptr(unsafe.Pointer(getg().m))
}

var extram uintptr
var extraMCount uint32 // Protected by lockextra
var extraMWaiters uint32

// lockextra locks the extra list and returns the list head.
// The caller must unlock the list by storing a new list head
// to extram. If nilokay is true, then lockextra will
// return a nil list head if that's what it finds. If nilokay is false,
// lockextra will keep waiting until the list head is no longer nil.
//go:nosplit
func lockextra(nilokay bool) *m {
	const locked = 1

	incr := false
	for {
		old := atomic.Loaduintptr(&extram)
		if old == locked {
			osyield()
			continue
		}
		if old == 0 && !nilokay {
			if !incr {
				// Add 1 to the number of threads
				// waiting for an M.
				// This is cleared by newextram.
				atomic.Xadd(&extraMWaiters, 1)
				incr = true
			}
			usleep(1)
			continue
		}
		if atomic.Casuintptr(&extram, old, locked) {
			return (*m)(unsafe.Pointer(old))
		}
		osyield()
		continue
	}
}

//go:nosplit
func unlockextra(mp *m) {
	atomic.Storeuintptr(&extram, uintptr(unsafe.Pointer(mp)))
}

// execLock serializes exec and clone to avoid bugs or unspecified behaviour
// around exec'ing while creating/destroying threads.  See issue #19546.
var execLock rwmutex

// newmHandoff contains a list of m structures that need new OS threads.
// This is used by newm in situations where newm itself can't safely
// start an OS thread.
var newmHandoff struct {
	lock mutex

	// newm points to a list of M structures that need new OS
	// threads. The list is linked through m.schedlink.
	newm muintptr

	// waiting indicates that wake needs to be notified when an m
	// is put on the list.
	waiting bool
	wake    note

	// haveTemplateThread indicates that the templateThread has
	// been started. This is not protected by lock. Use cas to set
	// to 1.
	haveTemplateThread uint32
}

// Create a new m. It will start off with a call to fn, or else the scheduler.
// fn needs to be static and not a heap allocated closure.
// May run with m.p==nil, so write barriers are not allowed.
//go:nowritebarrierrec
func newm(fn func(), _p_ *p) {
	// 调用 allocm 函数分配一个新的 M，并将其与 P 和 fn 关联。
	mp := allocm(_p_, fn)
	// 设置 M 的 nextp 为传入的 P。
	mp.nextp.set(_p_)
	// 初始化 M 的信号掩码 sigmask 为 initSigmask。
	mp.sigmask = initSigmask
	if gp := getg(); gp != nil && gp.m != nil && (gp.m.lockedExt != 0 || gp.m.incgo) && GOOS != "plan9" {
		// We're on a locked M or a thread that may have been
		// started by C. The kernel state of this thread may
		// be strange (the user may have locked it for that
		// purpose). We don't want to clone that into another
		// thread. Instead, ask a known-good thread to create
		// the thread for us.
		//
		// This is disabled on Plan 9. See golang.org/issue/22227.
		//
		// TODO: This may be unnecessary on Windows, which
		// doesn't model thread creation off fork.
		lock(&newmHandoff.lock)
		if newmHandoff.haveTemplateThread == 0 {
			throw("on a locked thread with no template thread")
		}
		mp.schedlink = newmHandoff.newm
		newmHandoff.newm.set(mp)
		if newmHandoff.waiting {
			newmHandoff.waiting = false
			notewakeup(&newmHandoff.wake)
		}
		unlock(&newmHandoff.lock)
		return
	}
	// 如果当前 Goroutine 不在锁定的 M 上运行，或者不是由 C 启动的线程，则直接调用 newm1 函数启动新的 M。
	newm1(mp)
}

func newm1(mp *m) {
	if iscgo {
		var ts cgothreadstart
		if _cgo_thread_start == nil {
			throw("_cgo_thread_start missing")
		}
		ts.g.set(mp.g0)
		ts.tls = (*uint64)(unsafe.Pointer(&mp.tls[0]))
		ts.fn = unsafe.Pointer(funcPC(mstart))
		if msanenabled {
			msanwrite(unsafe.Pointer(&ts), unsafe.Sizeof(ts))
		}
		execLock.rlock() // Prevent process clone.
		asmcgocall(_cgo_thread_start, unsafe.Pointer(&ts))
		execLock.runlock()
		return
	}
	execLock.rlock() // Prevent process clone.
	newosproc(mp)
	execLock.runlock()
}

// startTemplateThread starts the template thread if it is not already
// running.
//
// The calling thread must itself be in a known-good state.
func startTemplateThread() {
	if GOARCH == "wasm" { // no threads on wasm yet
		return
	}

	// Disable preemption to guarantee that the template thread will be
	// created before a park once haveTemplateThread is set.
	mp := acquirem()
	if !atomic.Cas(&newmHandoff.haveTemplateThread, 0, 1) {
		releasem(mp)
		return
	}
	newm(templateThread, nil)
	releasem(mp)
}

// templateThread is a thread in a known-good state that exists solely
// to start new threads in known-good states when the calling thread
// may not be in a good state.
//
// Many programs never need this, so templateThread is started lazily
// when we first enter a state that might lead to running on a thread
// in an unknown state.
//
// templateThread runs on an M without a P, so it must not have write
// barriers.
//
//go:nowritebarrierrec
func templateThread() {
	lock(&sched.lock)
	sched.nmsys++
	checkdead()
	unlock(&sched.lock)

	for {
		lock(&newmHandoff.lock)
		for newmHandoff.newm != 0 {
			newm := newmHandoff.newm.ptr()
			newmHandoff.newm = 0
			unlock(&newmHandoff.lock)
			for newm != nil {
				next := newm.schedlink.ptr()
				newm.schedlink = 0
				newm1(newm)
				newm = next
			}
			lock(&newmHandoff.lock)
		}
		newmHandoff.waiting = true
		noteclear(&newmHandoff.wake)
		unlock(&newmHandoff.lock)
		notesleep(&newmHandoff.wake)
	}
}

// Stops execution of the current m until new work is available.
// Returns with acquired P.
func stopm() {
	_g_ := getg()

	if _g_.m.locks != 0 {
		throw("stopm holding locks")
	}
	if _g_.m.p != 0 {
		throw("stopm holding p")
	}
	if _g_.m.spinning {
		throw("stopm spinning")
	}

	lock(&sched.lock)
	mput(_g_.m)
	unlock(&sched.lock)
	notesleep(&_g_.m.park)
	noteclear(&_g_.m.park)
	acquirep(_g_.m.nextp.ptr())
	_g_.m.nextp = 0
}

func mspinning() {
	// startm's caller incremented nmspinning. Set the new M's spinning.
	getg().m.spinning = true
}

/*
	用于调度一个 M（线程）来运行一个 P（处理器），如果必要的话会创建一个新的 M。
*/
// Schedules some M to run the p (creates an M if necessary).
// If p==nil, tries to get an idle P, if no idle P's does nothing.
// May run with m.p==nil, so write barriers are not allowed.
// If spinning is set, the caller has incremented nmspinning and startm will
// either decrement nmspinning or set m.spinning in the newly started M.
//go:nowritebarrierrec
func startm(_p_ *p, spinning bool) {
	lock(&sched.lock)
	if _p_ == nil {
		// 从空闲p列表中获取p
		_p_ = pidleget()
		if _p_ == nil {
			// 如果没有空闲的 P，解锁并检查 spinning 标志。
			unlock(&sched.lock)
			if spinning {
				// The caller incremented nmspinning, but there are no idle Ps,
				// so it's okay to just undo the increment and give up.
				// 如果 spinning 被设置，调用者已经增加了 nmspinning，因此需要减少 nmspinning 并放弃操作。
				// 如果减少 nmspinning 后的值小于 0，抛出异常 throw("startm: negative nmspinning")。
				if int32(atomic.Xadd(&sched.nmspinning, -1)) < 0 {
					throw("startm: negative nmspinning")
				}
			}
			return
		}
	}
	// 尝试从空闲m列表中，获取一个 M（mget）。
	mp := mget()
	// 解锁 sched.lock。
	unlock(&sched.lock)
	// 如果没有可用的 M，创建一个新的 M（newm），并根据 spinning 标志设置 m.spinning。
	if mp == nil {
		var fn func()
		if spinning {
			// The caller incremented nmspinning, so set m.spinning in the new M.
			fn = mspinning
		}
		// 这里会调用到C库的pthread_create函数，来创建系统线程
		newm(fn, _p_)
		// 这里创建完就直接返回了
		return
	}
	if mp.spinning {
		throw("startm: m is spinning")
	}
	if mp.nextp != 0 {
		throw("startm: m has p")
	}
	if spinning && !runqempty(_p_) {
		throw("startm: p has runnable gs")
	}
	// The caller incremented nmspinning, so set m.spinning in the new M.
	mp.spinning = spinning
	mp.nextp.set(_p_)
	// 唤醒 M（notewakeup(&mp.park)）。
	// 不是调用C库的pthread_create创建的线程，才需要唤醒操作。
	notewakeup(&mp.park)
}

// Hands off P from syscall or locked M.
// Always runs without a P, so write barriers are not allowed.
// handoffp 函数是 Go 运行时系统中的一个关键函数，用于在某些情况下将 P（Processor）从一个 M（Machine）移交给另一个 M。
// 这个函数在没有 P 的情况下运行，因此不允许使用写屏障（write barriers）。写屏障是垃圾回收机制的一部分，用于跟踪指针的写操作，以便正确地进行垃圾回收。

// handoffp 函数的主要作用是将一个 P 从一个 M 移交给另一个 M，以确保 P 能够继续执行 Goroutine。
// 这个函数通常在以下情况下被调用：
// 1. 当一个 M 进入系统调用并且需要释放其持有的 P。
// 2. 当一个 M 被锁定到一个 G 并且需要释放其持有的 P。

//go:nowritebarrierrec
func handoffp(_p_ *p) {
	// handoffp must start an M in any situation where
	// findrunnable would return a G to run on _p_.

	// if it has local work, start it straight away
	// 检查 _p_ 的本地工作队列是否为空。如果不为空，表示 _p_ 有本地的 Goroutine 需要处理。
	// 检查全局工作队列的大小是否为零。如果不为零，表示全局工作队列中有 Goroutine 需要处理。
	if !runqempty(_p_) || sched.runqsize != 0 {
		// 调用 startm 函数启动一个新的 M，并将 _p_ 传递给它。
		// 第二个参数 false 表示不强制创建一个新的 M，而是尝试复用现有的 M。
		startm(_p_, false)
		return
	}
	// if it has GC work, start it straight away
	if gcBlackenEnabled != 0 && gcMarkWorkAvailable(_p_) {
		startm(_p_, false)
		return
	}
	// no local work, check that there are no spinning/idle M's,
	// otherwise our help is not required
	if atomic.Load(&sched.nmspinning)+atomic.Load(&sched.npidle) == 0 && atomic.Cas(&sched.nmspinning, 0, 1) { // TODO: fast atomic
		startm(_p_, true)
		return
	}
	lock(&sched.lock)
	if sched.gcwaiting != 0 {
		_p_.status = _Pgcstop
		sched.stopwait--
		if sched.stopwait == 0 {
			notewakeup(&sched.stopnote)
		}
		unlock(&sched.lock)
		return
	}
	if _p_.runSafePointFn != 0 && atomic.Cas(&_p_.runSafePointFn, 1, 0) {
		sched.safePointFn(_p_)
		sched.safePointWait--
		if sched.safePointWait == 0 {
			notewakeup(&sched.safePointNote)
		}
	}
	if sched.runqsize != 0 {
		unlock(&sched.lock)
		startm(_p_, false)
		return
	}
	// If this is the last running P and nobody is polling network,
	// need to wakeup another M to poll network.
	if sched.npidle == uint32(gomaxprocs-1) && atomic.Load64(&sched.lastpoll) != 0 {
		unlock(&sched.lock)
		startm(_p_, false)
		return
	}
	if when := nobarrierWakeTime(_p_); when != 0 {
		wakeNetPoller(when)
	}
	pidleput(_p_)
	unlock(&sched.lock)
}

// 用于在一个新的 Goroutine 变为可运行状态时尝试增加一个处理器（P）来执行 Goroutine
// Tries to add one more P to execute G's.
// Called when a G is made runnable (newproc, ready).
func wakep() {
	// 使用原子操作 atomic.Load 检查全局空闲处理器计数 sched.npidle。
	// 如果没有空闲的 P（sched.npidle 为 0），则直接返回，不做任何操作。
	if atomic.Load(&sched.npidle) == 0 {
		return
	}
	// be conservative about spinning threads
	// 如果当前有自旋线程（sched.nmspinning 不为 0），即有自旋的M，直接返回，不做任何操作。
	// 如果没有自旋线程，尝试将 sched.nmspinning 从 0 设置为 1，表示将启动一个新的自旋线程。启动失败也返回。
	if atomic.Load(&sched.nmspinning) != 0 || !atomic.Cas(&sched.nmspinning, 0, 1) {
		return
	}
	// 如果没有空闲p，这个函数会直接返回，不会开启新的m
	startm(nil, true)
}

/*
这段代码是 Go 运行时系统中的一部分，用于处理一个特定的情况：
当一个 M（Machine）被锁定到一个 G（Goroutine）时，停止该 M 的执行，直到该 G 再次变为可运行状态。

stoplockedm 函数在 Go 运行时系统中用于处理 M 和 G 之间的锁定关系。
当一个 M 被锁定到一个 G 时，如果 G 需要等待某些条件满足，stoplockedm 会停止该 M 的执行，直到 G 再次变为可运行状态。
这个函数确保了锁定状态的一致性，并在必要时将 P 交给其他 M 运行，以保持系统的高效运行。
*/
// Stops execution of the current m that is locked to a g until the g is runnable again.
// Returns with acquired P.
func stoplockedm() {
	_g_ := getg()
	// 检查当前 M 是否锁定了一个 G，并且该 G 是否也锁定了当前 M。如果不满足这些条件，则抛出异常，表示锁定状态不一致
	if _g_.m.lockedg == 0 || _g_.m.lockedg.ptr().lockedm.ptr() != _g_.m {
		throw("stoplockedm: inconsistent locking")
	}
	// 如果当前 M 关联了一个 P（Processor），则释放该 P，并将其交给另一个 M 运行。
	if _g_.m.p != 0 {
		// Schedule another M to run this p.
		// 释放p
		_p_ := releasep()
		// 将p交给其他m执行
		handoffp(_p_)
	}
	// 增加空闲锁定的计数。
	incidlelocked(1)
	// Wait until another thread schedules lockedg again.
	// 使当前 M 进入休眠状态，直到另一个线程再次调度锁定的 G。
	// 【这里会阻塞住，里面是个for循环】
	notesleep(&_g_.m.park)
	// 锁定G被唤醒，M可以继续执行，所以清除休眠标记。
	noteclear(&_g_.m.park)
	// 读取锁定的 G 的状态。
	status := readgstatus(_g_.m.lockedg.ptr())
	// 检查锁定的 G 是否处于可运行状态（_Grunnable）或扫描可运行状态（_Gscanrunnable）。如果不是，则打印错误信息并抛出异常。
	if status&^_Gscan != _Grunnable {
		print("runtime:stoplockedm: g is not Grunnable or Gscanrunnable\n")
		dumpgstatus(_g_)
		throw("stoplockedm: not runnable")
	}
	// acquirep 函数将 nextp 指向的 P 重新与当前 M 关联。这意味着当前 M 将继续使用这个 P 来执行 Goroutine。
	acquirep(_g_.m.nextp.ptr())
	// 将 nextp 设置为 0 是为了清除这个临时存储的 P。这样做的目的是确保 nextp 不再指向任何 P，避免在后续操作中产生混淆或错误。
	_g_.m.nextp = 0
}

/*
	startlockedm 函数的目的是调度一个锁定的 M 来运行一个锁定的 G。
	该函数可能在 STW（Stop The World）期间运行，因此不允许写屏障（go:nowritebarrierrec）。
*/
// Schedules the locked m to run the locked gp.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func startlockedm(gp *g) {
	// 获取当前的 Goroutine _g_。
	_g_ := getg()
	// 获取锁定的 M，gp.lockedm 是指向锁定 M 的指针。
	mp := gp.lockedm.ptr()
	// 如果锁定的 M 是当前 M，抛出异常 throw("startlockedm: locked to me")。
	if mp == _g_.m {
		throw("startlockedm: locked to me")
	}
	// 如果锁定的 M 已经有 P，抛出异常 throw("startlockedm: m has p")。
	if mp.nextp != 0 {
		throw("startlockedm: m has p")
	}
	// 直接将当前 P 交给锁定的 M
	// directly handoff current P to the locked m
	// 调用 incidlelocked(-1) 函数减少空闲锁定 M 的计数。
	incidlelocked(-1)
	// 调用 releasep() 函数释放当前 M 持有的 P，并将其返回给 _p_。
	_p_ := releasep()
	// 将 P 设置为锁定的 M 的 nextp。
	mp.nextp.set(_p_)
	// 调用 notewakeup(&mp.park) 函数唤醒锁定的 M。
	notewakeup(&mp.park)
	// 调用 stopm() 函数停止当前 M。
	stopm()
}

// Stops the current m for stopTheWorld.
// Returns when the world is restarted.
func gcstopm() {
	_g_ := getg()

	if sched.gcwaiting == 0 {
		throw("gcstopm: not waiting for gc")
	}
	if _g_.m.spinning {
		_g_.m.spinning = false
		// OK to just drop nmspinning here,
		// startTheWorld will unpark threads as necessary.
		if int32(atomic.Xadd(&sched.nmspinning, -1)) < 0 {
			throw("gcstopm: negative nmspinning")
		}
	}
	_p_ := releasep()
	lock(&sched.lock)
	_p_.status = _Pgcstop
	sched.stopwait--
	if sched.stopwait == 0 {
		notewakeup(&sched.stopnote)
	}
	unlock(&sched.lock)
	stopm()
}

/*
	这段代码是 Go 运行时调度器的一部分，用于将一个 goroutine（gp）调度到当前线程（M）上运行。
	它处理了 goroutine 的状态转换、时间片继承、性能分析器的设置以及跟踪事件的记录。
*/
// 将 gp 调度到当前线程（M）上运行。
// 如果 inheritTime 为真，gp 继承当前时间片的剩余时间。否则，它将开始一个新的时间片。
// 该函数不会返回。
// 允许写屏障，因为这是在获取 P 之后立即调用的。
// go:yeswritebarrierrec：编译器指令，允许在此函数中使用写屏障。
// Schedules gp to run on the current M.
// If inheritTime is true, gp inherits the remaining time in the
// current time slice. Otherwise, it starts a new time slice.
// Never returns.
//
// Write barriers are allowed because this is called immediately after
// acquiring a P in several places.
//
//go:yeswritebarrierrec
func execute(gp *g, inheritTime bool) {
	// 获取当前 goroutine
	_g_ := getg()

	// Assign gp.m before entering _Grunning so running Gs have an
	// M.
	// 将当前M和要执行的G相互绑定：
	// 1. 将当前线程（M）的 curg 设置为 gp。
	_g_.m.curg = gp
	// 2. 将 gp 的 m 设置为当前线程（M）。
	gp.m = _g_.m
	// 将 gp 的状态从 _Grunnable（可运行）设置为 _Grunning（运行中）。
	casgstatus(gp, _Grunnable, _Grunning)
	// 重置 gp 的等待时间。
	gp.waitsince = 0
	// 重置 gp 的抢占标志为：不允许抢占
	gp.preempt = false
	// 设置 gp 的栈保护区。
	gp.stackguard0 = gp.stack.lo + _StackGuard
	// 如果不继承时间片，增加当前处理器（P）的调度计数。
	if !inheritTime {
		_g_.m.p.ptr().schedtick++
	}

	// 设置性能分析器：
	// Check whether the profiler needs to be turned on or off.
	hz := sched.profilehz
	if _g_.m.profilehz != hz {
		setThreadCPUProfiler(hz)
	}

	// 处理跟踪事件：
	if trace.enabled {
		// GoSysExit has to happen when we have a P, but before GoStart.
		// So we emit it here.
		if gp.syscallsp != 0 && gp.sysblocktraced {
			traceGoSysExit(gp.sysexitticks)
		}
		traceGoStart()
	}
	// 在 Go 语言中，gogo 是一个低级别的运行时函数，通常用汇编语言实现。它的主要作用是切换到指定的 goroutine 并开始执行该 goroutine 的代码。
	gogo(&gp.sched)
}

// Finds a runnable goroutine to execute.
// Tries to steal from other P's, get g from local or global queue, poll network.
func findrunnable() (gp *g, inheritTime bool) {
	_g_ := getg()

	// The conditions here and in handoffp must agree: if
	// findrunnable would return a G to run, handoffp must start
	// an M.
	// findrunnable 函数和 handoffp 函数之间的条件必须一致。
	// 如果 findrunnable 函数返回一个可运行的 goroutine，handoffp 函数必须启动一个新的 M（线程）。

top:
	// 获取当前 M 绑定的 P，并将其赋值给 _p_。
	_p_ := _g_.m.p.ptr()
	// 检查全局调度器的 gcwaiting 标志是否被设置。如果被设置，表示需要进行垃圾回收。
	if sched.gcwaiting != 0 {
		// 调用 gcstopm 函数，停止当前 M 以便进行垃圾回收。
		gcstopm()
		goto top
	}
	// 检查当前 P 是否有需要运行的安全点函数。
	// 补充：安全点函数：在某些情况下，Go 运行时系统需要在特定的安全点运行一些函数，以确保系统的一致性和安全性。
	if _p_.runSafePointFn != 0 {
		runSafePointFn()
	}

	// 调用 checkTimers 函数，检查当前 P 的定时器。checkTimers 返回当前时间 now、下一个定时器触发时间 pollUntil
	now, pollUntil, _ := checkTimers(_p_, 0)

	// 补充：Finalizer 是一个函数，当某个对象即将被垃圾回收时，Go 运行时系统会调用这个函数。finalizer 通常用于释放非 Go 语言的资源，例如关闭文件描述符、释放 C 语言分配的内存等。
	//      finalizer goroutine 是负责执行这些 finalizer 函数的 goroutine。
	// 检查 fingwait 和 fingwake 标志是否都被设置。如果都被设置，表示有等待的 finalizer goroutine 需要被唤醒。
	if fingwait && fingwake {
		// 调用 wakefing 函数，尝试唤醒一个 finalizer goroutine。如果成功唤醒，gp 不为 nil。
		if gp := wakefing(); gp != nil {
			// 用 ready 函数，将唤醒的 goroutine 标记为可运行，并将其放入运行队列。
			ready(gp, 0, true)
		}
	}
	// 检查 cgo_yield 是否被设置。如果被设置，表示需要进行 cgo 的让步。
	if *cgo_yield != nil {
		// 调用 asmcgocall 函数，执行 cgo 的让步操作。
		asmcgocall(*cgo_yield, nil)
	}

	// local runq
	// 1. 从本地队列取G
	if gp, inheritTime := runqget(_p_); gp != nil {
		return gp, inheritTime
	}

	// global runq
	// 2. 从全局队列取G
	if sched.runqsize != 0 {
		lock(&sched.lock)
		gp := globrunqget(_p_, 0)
		unlock(&sched.lock)
		if gp != nil {
			return gp, false
		}
	}

	/*
		网络轮询是 Go 调度器中的一个优化步骤，用于在尝试从其他处理器（P）中窃取 Goroutine 之前，先检查是否有任何网络事件已经准备好处理。

		这个网络轮询只是一个优化步骤，在我们尝试从其他处理器中窃取 Goroutine 之前进行。
		如果没有任何等待的G，或者netpoll中的线程已经阻塞，我们可以安全地跳过它。
		如果与那个被阻塞的线程存在任何逻辑上的竞争（例如，它已经从网络轮询返回，但还没有设置 lastpoll），这个线程无论如何都会在下面进行阻塞的网络轮询。
	*/
	// Poll network.
	// This netpoll is only an optimization before we resort to stealing.
	// We can safely skip it if there are no waiters or a thread is blocked in netpoll already.
	// If there is any kind of logical race with that blocked thread (e.g. it has already returned from netpoll, but does not set lastpoll yet), this thread will do blocking netpoll below anyway.

	/*
				设计意图：
				1. 优化调度：通过在尝试从其他处理器中窃取 Goroutine 之前进行网络轮询，可以更快地找到可运行的 Goroutine，从而优化调度性能。
			    2. 减少阻塞：非阻塞的网络轮询可以避免线程阻塞，提高系统的响应速度。
				3. 并发安全：使用原子操作检查条件，确保在多线程环境下的并发安全。

				总结：
				这段代码通过进行非阻塞的网络轮询，尝试找到已经准备好处理的网络事件对应的 Goroutine。
		  		如果找到了可运行的 Goroutine，它会将其状态更改为可运行，并返回该 Goroutine。
				这个步骤是 Go 调度器中的一个优化步骤，用于在尝试从其他处理器中窃取 Goroutine 之前，先检查是否有任何网络事件已经准备好处理。
		 		通过这种方式，可以更快地找到可运行的 Goroutine，从而优化调度性能并减少线程阻塞。
	*/

	// 3. 从网络轮询器中获取G
	// 检查网络轮询是否已初始化。
	// 检查是否有等待网络事件的 Goroutine。
	// 检查 lastpoll 是否已设置，确保没有其他线程正在进行网络轮询。
	if netpollinited() && atomic.Load(&netpollWaiters) > 0 && atomic.Load64(&sched.lastpoll) != 0 {
		// 调用 netpoll(0) 进行非阻塞的网络轮询，检查是否有任何网络事件已经准备好处理。
		// 如果返回的列表 list 不为空，表示有网络事件已经准备好。
		if list := netpoll(0); !list.empty() { // non-blocking
			// 从列表中弹出一个 Goroutine gp。
			gp := list.pop()
			// 将剩余的 Goroutine 列表注入到全局队列或本地队列中
			injectglist(&list)
			// 将 gp 的状态从 _Gwaiting 更改为 _Grunnable，表示它现在可以运行。
			casgstatus(gp, _Gwaiting, _Grunnable)
			if trace.enabled {
				// 如果跟踪（trace）功能启用，记录跟踪信息。
				traceGoUnpark(gp, 0)
			}
			// 返回 gp 和 false，表示找到了一个可运行的 Goroutine。
			return gp, false
		}
	}

	// Steal work from other P's.
	// 当前系统中最大处理器数（GOMAXPROCS）。
	procs := uint32(gomaxprocs)
	// 标记是否运行了定时器。
	ranTimer := false
	// If number of spinning M's >= number of busy P's, block.
	// This is necessary to prevent excessive CPU consumption
	// when GOMAXPROCS>>1 but the program parallelism is low.
	// 如果当前 M 没有在自旋，并且自旋的 M 数量大于等于忙碌的 P 数量，则阻塞当前 M。
	// 这样做是为了防止在 GOMAXPROCS 远大于 1 但程序并行度较低时，过度的 CPU 消耗。
	if !_g_.m.spinning && 2*atomic.Load(&sched.nmspinning) >= procs-atomic.Load(&sched.npidle) {
		goto stop
	}
	// 如果当前 M 没有在自旋，将其设置为自旋状态，并增加自旋的 M 数量。
	if !_g_.m.spinning {
		_g_.m.spinning = true
		atomic.Xadd(&sched.nmspinning, 1)
	}
	// 外层循环尝试 4 次从其他 P 中窃取工作。
	for i := 0; i < 4; i++ {
		// 内层循环遍历所有 P，尝试从中窃取工作。
		for enum := stealOrder.start(fastrand()); !enum.done(); enum.next() {
			// 如果垃圾回收正在等待，跳转到 top 重新开始。
			if sched.gcwaiting != 0 {
				goto top
			}
			// stealRunNextG：在前两次尝试中，只窃取准备队列中有多个 Goroutine 的 P。
			// 即前两轮不窃取 runNextG
			stealRunNextG := i > 2 // first look for ready queues with more than 1 g
			// 当前尝试窃取工作的 P。
			p2 := allp[enum.position()]
			if _p_ == p2 {
				continue
			}
			// 尝试从 p2 中窃取 Goroutine，如果成功，返回窃取到的 Goroutine。
			if gp := runqsteal(_p_, p2, stealRunNextG); gp != nil {
				return gp, false
			}

			// Consider stealing timers from p2.
			// This call to checkTimers is the only place where
			// we hold a lock on a different P's timers.
			// Lock contention can be a problem here, so
			// initially avoid grabbing the lock if p2 is running
			// and is not marked for preemption. If p2 is running
			// and not being preempted we assume it will handle its
			// own timers.
			// If we're still looking for work after checking all
			// the P's, then go ahead and steal from an active P.
			// 在第三次或第四次尝试时，
			// 或者在第二次尝试且 shouldStealTimers(p2) 返回 true 时，尝试窃取定时器任务。
			if i > 2 || (i > 1 && shouldStealTimers(p2)) {
				// 调用 checkTimers 检查 p2 的定时器任务。
				// tnow：当前时间。
				// w：下一个定时器的等待时间。
				// ran：是否运行了定时器任务。
				tnow, w, ran := checkTimers(p2, now)
				now = tnow
				if w != 0 && (pollUntil == 0 || w < pollUntil) {
					pollUntil = w
				}
				if ran {
					// 如果运行了定时器任务，可能有新的 Goroutine 被添加到本地运行队列。
					// Running the timers may have
					// made an arbitrary number of G's
					// ready and added them to this P's
					// local run queue. That invalidates
					// the assumption of runqsteal
					// that is always has room to add
					// stolen G's. So check now if there
					// is a local G to run.
					// 尝试从本地运行队列中获取一个 Goroutine。
					// 如果成功，返回这个 Goroutine 及其继承的时间。
					if gp, inheritTime := runqget(_p_); gp != nil {
						return gp, inheritTime
					}
					ranTimer = true
				}
			}
		}
	}
	if ranTimer {
		// Running a timer may have made some goroutine ready.
		goto top
	}

stop:

	// We have nothing to do. If we're in the GC mark phase, can
	// safely scan and blacken objects, and have work to do, run
	// idle-time marking rather than give up the P.
	if gcBlackenEnabled != 0 && _p_.gcBgMarkWorker != 0 && gcMarkWorkAvailable(_p_) {
		_p_.gcMarkWorkerMode = gcMarkWorkerIdleMode
		gp := _p_.gcBgMarkWorker.ptr()
		casgstatus(gp, _Gwaiting, _Grunnable)
		if trace.enabled {
			traceGoUnpark(gp, 0)
		}
		return gp, false
	}

	delta := int64(-1)
	if pollUntil != 0 {
		// checkTimers ensures that polluntil > now.
		delta = pollUntil - now
	}

	// wasm only:
	// If a callback returned and no other goroutine is awake,
	// then wake event handler goroutine which pauses execution
	// until a callback was triggered.
	gp, otherReady := beforeIdle(delta)
	if gp != nil {
		casgstatus(gp, _Gwaiting, _Grunnable)
		if trace.enabled {
			traceGoUnpark(gp, 0)
		}
		return gp, false
	}
	if otherReady {
		goto top
	}

	// Before we drop our P, make a snapshot of the allp slice,
	// which can change underfoot once we no longer block
	// safe-points. We don't need to snapshot the contents because
	// everything up to cap(allp) is immutable.
	allpSnapshot := allp

	// return P and block
	lock(&sched.lock)
	if sched.gcwaiting != 0 || _p_.runSafePointFn != 0 {
		unlock(&sched.lock)
		goto top
	}
	if sched.runqsize != 0 {
		gp := globrunqget(_p_, 0)
		unlock(&sched.lock)
		return gp, false
	}
	if releasep() != _p_ {
		throw("findrunnable: wrong p")
	}
	pidleput(_p_)
	unlock(&sched.lock)

	// Delicate dance: thread transitions from spinning to non-spinning state,
	// potentially concurrently with submission of new goroutines. We must
	// drop nmspinning first and then check all per-P queues again (with
	// #StoreLoad memory barrier in between). If we do it the other way around,
	// another thread can submit a goroutine after we've checked all run queues
	// but before we drop nmspinning; as the result nobody will unpark a thread
	// to run the goroutine.
	// If we discover new work below, we need to restore m.spinning as a signal
	// for resetspinning to unpark a new worker thread (because there can be more
	// than one starving goroutine). However, if after discovering new work
	// we also observe no idle Ps, it is OK to just park the current thread:
	// the system is fully loaded so no spinning threads are required.
	// Also see "Worker thread parking/unparking" comment at the top of the file.
	wasSpinning := _g_.m.spinning
	if _g_.m.spinning {
		_g_.m.spinning = false
		if int32(atomic.Xadd(&sched.nmspinning, -1)) < 0 {
			throw("findrunnable: negative nmspinning")
		}
	}

	// check all runqueues once again
	for _, _p_ := range allpSnapshot {
		if !runqempty(_p_) {
			lock(&sched.lock)
			_p_ = pidleget()
			unlock(&sched.lock)
			if _p_ != nil {
				acquirep(_p_)
				if wasSpinning {
					_g_.m.spinning = true
					atomic.Xadd(&sched.nmspinning, 1)
				}
				goto top
			}
			break
		}
	}

	// Check for idle-priority GC work again.
	if gcBlackenEnabled != 0 && gcMarkWorkAvailable(nil) {
		lock(&sched.lock)
		_p_ = pidleget()
		if _p_ != nil && _p_.gcBgMarkWorker == 0 {
			pidleput(_p_)
			_p_ = nil
		}
		unlock(&sched.lock)
		if _p_ != nil {
			acquirep(_p_)
			if wasSpinning {
				_g_.m.spinning = true
				atomic.Xadd(&sched.nmspinning, 1)
			}
			// Go back to idle GC check.
			goto stop
		}
	}

	// poll network
	if netpollinited() && (atomic.Load(&netpollWaiters) > 0 || pollUntil != 0) && atomic.Xchg64(&sched.lastpoll, 0) != 0 {
		atomic.Store64(&sched.pollUntil, uint64(pollUntil))
		if _g_.m.p != 0 {
			throw("findrunnable: netpoll with p")
		}
		if _g_.m.spinning {
			throw("findrunnable: netpoll with spinning")
		}
		if faketime != 0 {
			// When using fake time, just poll.
			delta = 0
		}
		list := netpoll(delta) // block until new work is available
		atomic.Store64(&sched.pollUntil, 0)
		atomic.Store64(&sched.lastpoll, uint64(nanotime()))
		if faketime != 0 && list.empty() {
			// Using fake time and nothing is ready; stop M.
			// When all M's stop, checkdead will call timejump.
			stopm()
			goto top
		}
		lock(&sched.lock)
		_p_ = pidleget()
		unlock(&sched.lock)
		if _p_ == nil {
			injectglist(&list)
		} else {
			acquirep(_p_)
			if !list.empty() {
				gp := list.pop()
				injectglist(&list)
				casgstatus(gp, _Gwaiting, _Grunnable)
				if trace.enabled {
					traceGoUnpark(gp, 0)
				}
				return gp, false
			}
			if wasSpinning {
				_g_.m.spinning = true
				atomic.Xadd(&sched.nmspinning, 1)
			}
			goto top
		}
	} else if pollUntil != 0 && netpollinited() {
		pollerPollUntil := int64(atomic.Load64(&sched.pollUntil))
		if pollerPollUntil == 0 || pollerPollUntil > pollUntil {
			netpollBreak()
		}
	}
	stopm()
	goto top
}

// pollWork reports whether there is non-background work this P could
// be doing. This is a fairly lightweight check to be used for
// background work loops, like idle GC. It checks a subset of the
// conditions checked by the actual scheduler.
func pollWork() bool {
	if sched.runqsize != 0 {
		return true
	}
	p := getg().m.p.ptr()
	if !runqempty(p) {
		return true
	}
	if netpollinited() && atomic.Load(&netpollWaiters) > 0 && sched.lastpoll != 0 {
		if list := netpoll(0); !list.empty() {
			injectglist(&list)
			return true
		}
	}
	return false
}

// wakeNetPoller wakes up the thread sleeping in the network poller,
// if there is one, and if it isn't going to wake up anyhow before
// the when argument.
func wakeNetPoller(when int64) {
	if atomic.Load64(&sched.lastpoll) == 0 {
		// In findrunnable we ensure that when polling the pollUntil
		// field is either zero or the time to which the current
		// poll is expected to run. This can have a spurious wakeup
		// but should never miss a wakeup.
		pollerPollUntil := int64(atomic.Load64(&sched.pollUntil))
		if pollerPollUntil == 0 || pollerPollUntil > when {
			netpollBreak()
		}
	}
}

// 用于重置一个线程（M）的自旋状态，并根据需要唤醒另一个处理器（P）。
func resetspinning() {
	_g_ := getg()
	if !_g_.m.spinning {
		throw("resetspinning: not a spinning m")
	}
	_g_.m.spinning = false
	// 使用原子操作 atomic.Xadd 将全局自旋线程计数 sched.nmspinning 减 1。
	nmspinning := atomic.Xadd(&sched.nmspinning, -1)
	// 检查更新后的 nmspinning 是否为负数，如果是，抛出异常
	if int32(nmspinning) < 0 {
		throw("findrunnable: negative nmspinning")
	}
	// M wakeup policy is deliberately somewhat conservative, so check if we
	// need to wakeup another P here. See "Worker thread parking/unparking"
	// comment at the top of the file for details.
	// 调用 wakep 函数，根据需要唤醒另一个处理器（P）。
	wakep()
}

/*
设计意图
1. 负载均衡：通过将 Goroutine 添加到全局队列或本地队列，实现负载均衡，确保 Goroutine 能够被及时调度执行。
2. 并发安全：通过获取调度器锁，确保在多线程环境下对全局队列的访问是线程安全的。
3. 与 GC 并发运行：设计上允许与垃圾回收并发运行，避免在垃圾回收期间阻塞调度器。
*/
// injectglist adds each runnable G on the list to some run queue,
// and clears glist. If there is no current P, they are added to the
// global queue, and up to npidle M's are started to run them.
// Otherwise, for each idle P, this adds a G to the global queue
// and starts an M. Any remaining G's are added to the current P's
// local run queue.
// This may temporarily acquire the scheduler lock.
// Can run concurrently with GC.
func injectglist(glist *gList) {
	if glist.empty() {
		return
	}
	if trace.enabled {
		for gp := glist.head.ptr(); gp != nil; gp = gp.schedlink.ptr() {
			traceGoUnpark(gp, 0)
		}
	}

	// Mark all the goroutines as runnable before we put them
	// on the run queues.
	head := glist.head.ptr()
	var tail *g
	qsize := 0
	for gp := head; gp != nil; gp = gp.schedlink.ptr() {
		tail = gp
		qsize++
		casgstatus(gp, _Gwaiting, _Grunnable)
	}

	// Turn the gList into a gQueue.
	var q gQueue
	q.head.set(head)
	q.tail.set(tail)
	*glist = gList{}

	startIdle := func(n int) {
		for ; n != 0 && sched.npidle != 0; n-- {
			startm(nil, false)
		}
	}

	pp := getg().m.p.ptr()
	if pp == nil {
		lock(&sched.lock)
		globrunqputbatch(&q, int32(qsize))
		unlock(&sched.lock)
		startIdle(qsize)
		return
	}

	lock(&sched.lock)
	npidle := int(sched.npidle)
	var n int
	for n = 0; n < npidle && !q.empty(); n++ {
		globrunqput(q.pop())
	}
	unlock(&sched.lock)
	startIdle(n)
	qsize -= n

	if !q.empty() {
		runqputbatch(pp, &q, qsize)
	}
}

/*
schedule 函数是 Go 运行时系统中的核心调度函数。
它负责在调度循环中找到一个可运行的 goroutine 并执行它。
这个函数永远不会返回，因为它会不断地寻找和执行 goroutine。
*/
// One round of scheduler: find a runnable goroutine and execute it.
// Never returns.
// XGF Go调度器的逻辑
func schedule() {
	// 1. 这里获取到的G永远是g0。
	// 因为只有g0才有调度其他g的能力？
	// 是不是每个m，都有一个g0？
	// 答案：g0 是一个特殊的 Goroutine，它并不执行用户代码，而是用于执行调度器和运行时的内部任务。
	// 在 Go 的运行时系统中，每个 M（Machine）都有一个 g0，负责该 M 中的 Goroutine 的调度。
	_g_ := getg()

	if _g_.m.locks != 0 {
		// 如果当前 M（线程）持有锁，则抛出异常。调度器不应该在持有锁的情况下进行调度。
		throw("schedule: holding locks")
	}

	if _g_.m.lockedg != 0 {
		// 如果当前 M 锁定了一个 G，则停止当前 M，交出P，然后自旋等待锁定G被其他G调用/唤醒。
		stoplockedm()
		// 当锁定G被唤醒，则继续执行锁定G
		execute(_g_.m.lockedg.ptr(), false) // Never returns.
	}

	// We should not schedule away from a g that is executing a cgo call, since the cgo call is using the m's g0 stack.
	// 我们不应该将正在执行cgo调用的g安排在某个地方，因为cgo调用正在使用m的g0堆栈。
	if _g_.m.incgo {
		throw("schedule: in cgo")
	}
	// 调度循环的起点标签。
top:
	// 获取当前 M 绑定的 P。
	pp := _g_.m.p.ptr()
	pp.preempt = false

	// 2. 如果处于GC阶段，停止当前M
	if sched.gcwaiting != 0 {
		gcstopm()
		goto top
	}
	// 如果 P 有安全点函数需要运行，则运行该函数。
	if pp.runSafePointFn != 0 {
		runSafePointFn()
	}

	// Sanity check: if we are spinning, the run queue should be empty.
	// Check this before calling checkTimers, as that might call
	// goready to put a ready goroutine on the local run queue.
	// 检查自旋状态：
	// 如果 M 正在自旋且本地运行队列不为空，则抛出异常。
	if _g_.m.spinning && (pp.runnext != 0 || pp.runqhead != pp.runqtail) {
		throw("schedule: spinning with local work")
	}

	// checkTimers 函数用于检查定时器，并可能将定时器触发的 goroutine 放入本地运行队列。
	// goready 函数用于将一个 goroutine 标记为可运行，并将其放入本地或全局运行队列。
	checkTimers(pp, 0)

	// 下一个G
	var gp *g
	var inheritTime bool

	// Normal goroutines will check for need to wakeP in ready,
	// but GCworkers and tracereaders will not, so the check must
	// be done here instead.
	// tryWakeP 标志用于指示是否需要唤醒一个新的 P（处理器）来处理更多的 goroutine。
	tryWakeP := false
	// 如果跟踪启用或关闭，则尝试获取一个跟踪读取器 goroutine。
	if trace.enabled || trace.shutdown {
		gp = traceReader()
		if gp != nil {
			casgstatus(gp, _Gwaiting, _Grunnable)
			traceGoUnpark(gp, 0)
			tryWakeP = true
		}
	}
	// gcBlackenEnabled 是一个全局变量，用于指示垃圾回收的黑化阶段是否启用。
	// 黑化阶段是垃圾回收的一部分，在这个阶段，垃圾回收器会标记所有存活的对象。
	// 尝试找到一个可运行的 GC 工作 goroutine。
	// 这个方法会在当前 P（处理器）上寻找一个可运行的 GC 工作 goroutine，并将其赋值给 gp。
	if gp == nil && gcBlackenEnabled != 0 {
		gp = gcController.findRunnableGCWorker(_g_.m.p.ptr())
		tryWakeP = tryWakeP || gp != nil
	}
	if gp == nil {
		// Check the global runnable queue once in a while to ensure fairness.
		// Otherwise two goroutines can completely occupy the local runqueue
		// by constantly respawning each other.

		// 当前 P 的调度计数器 schedtick 是否是 61 的倍数。schedtick 是一个计数器，每次调度时递增一次。
		// 通过取模运算，可以确保每 61 次调度检查一次全局运行队列。

		// 为什么要检查全局运行队列？
		// 主要目的是为了确保调度的公平性。
		// 否则，两个 goroutine 可能会通过不断地相互生成新 goroutine 而完全占据本地运行队列，导致其他 goroutine 无法得到调度。
		if _g_.m.p.ptr().schedtick%61 == 0 && sched.runqsize > 0 {
			lock(&sched.lock)
			gp = globrunqget(_g_.m.p.ptr(), 1)
			unlock(&sched.lock)
		}
	}
	if gp == nil {
		// 从本地运行队列中获取一个可运行的 goroutine。
		gp, inheritTime = runqget(_g_.m.p.ptr())
		// 即使当前 M（线程）处于自旋状态，gp 也可能不为 nil。
		// 这是因为 checkTimers 函数可能通过 goready 将一个本地 goroutine 添加到本地运行队列中。
		// We can see gp != nil here even if the M is spinning,
		// if checkTimers added a local goroutine via goready.

		// 补充：自旋状态：自旋是指 M 在没有找到可运行的 goroutine 时，不立即进入休眠，而是进行短暂的忙等待，
		// 以期望在短时间内有新的 goroutine 变为可运行状态。自旋可以减少线程上下文切换的开销，提高性能。
	}
	if gp == nil {
		// 3. 取下一个处于可运行状态的G，按以下规则进行获取
		// 3.1 从本地队列取
		// 3.2 从全局队列取
		// 3.3 从其他线程中窃取
		gp, inheritTime = findrunnable() // blocks until work is available
	}

	// NOTE: Spinning（空转）：空转状态表示一个线程正在积极地寻找可运行的 goroutine，而不是阻塞等待。
	//		空转线程用于提高调度器的响应速度，但过多的空转线程会浪费 CPU 资源。
	// This thread is going to run a goroutine and is not spinning anymore,
	// so if it was marked as spinning we need to reset it now and potentially
	// start a new spinning M.
	// 这个线程即将运行一个 goroutine，因此它不再处于空转状态。
	// 如果该线程之前被标记为空转状态，我们需要重置这个状态，并可能启动一个新的空转线程。
	if _g_.m.spinning {
		// 调用 resetspinning 函数，重置当前线程的空转状态，并可能启动一个新的空转线程。
		resetspinning()
	}

	// 检查全局调度器状态 sched.disable.user 是否禁用了用户调度，并且当前 goroutine gp 是否不允许被调度（!schedEnabled(gp)）。
	if sched.disable.user && !schedEnabled(gp) {
		// Scheduling of this goroutine is disabled. Put it on
		// the list of pending runnable goroutines for when we
		// re-enable user scheduling and look again.
		// 调度这个 goroutine 被禁用了。将其放入待运行的 goroutine 列表中，以便在重新启用用户调度时再次检查。

		// 获取调度器的全局锁 sched.lock，以确保对调度器状态的修改是线程安全的。
		lock(&sched.lock)
		// 在获取锁之后，再次检查当前 goroutine 是否允许被调度
		if schedEnabled(gp) {
			// Something re-enabled scheduling while we
			// were acquiring the lock.
			// 在获取锁的过程中，调度状态可能已经被其他线程修改。
			// 如果调度状态已经被重新启用，释放锁并退出。
			unlock(&sched.lock)
		} else {
			// 将当前 goroutine gp 放入 sched.disable.runnable 列表的末尾。
			sched.disable.runnable.pushBack(gp)
			// 增加待运行 goroutine 的计数。
			sched.disable.n++
			// 释放调度器的全局锁。
			unlock(&sched.lock)
			// 跳转到代码的顶部，重新开始调度循环。
			goto top
		}
	}

	/*
		Note:
		用于处理即将调度的 goroutine 是否需要唤醒一个新的处理器（P），以及处理被锁定到特定线程（M）的 goroutine。
		GCworker 和 tracereader：这些是 Go 运行时系统中的特殊 goroutine，用于垃圾回收和跟踪。它们可能需要额外的处理器资源来确保系统的高效运行。
	*/
	// If about to schedule a not-normal goroutine (a GCworker or tracereader),
	// wake a P if there is one.
	// 检查是否需要唤醒一个新的处理器（P）。tryWakeP 是一个布尔值，表示是否需要唤醒一个新的 P。
	if tryWakeP {
		// 调用 wakep 函数，唤醒一个新的处理器（P）。这通常在即将调度一个非正常的 goroutine（如 GCworker 或 tracereader）时发生，以确保有足够的处理器来处理这些特殊的任务。
		wakep()
	}

	/*
		在 Go 运行时系统中，将一个 Goroutine（G）锁定到一个线程（M）通常是为了确保某些操作在特定的线程上下文中执行。这种情况在以下几种场景中可能会发生：
		1. 与操作系统线程相关的操作：
			某些操作需要在特定的操作系统线程上执行。例如，某些系统调用或库函数可能要求在调用线程上执行后续操作。
			在这种情况下，Go 运行时系统会将 Goroutine 锁定到特定的 M，以确保这些操作在同一个线程上执行。
		2. 线程本地存储（TLS）：
		    某些库或系统调用依赖于线程本地存储（TLS）。为了确保 Goroutine 在同一个线程上执行并访问正确的 TLS 数据，Go 运行时系统会将 Goroutine 锁定到特定的 M。
		3. 外部 C 代码的调用
			通过 cgo 调用外部 C 代码时，可能需要将 Goroutine 锁定到特定的 M，以确保 C 代码在同一个线程上执行。这是因为某些 C 库可能依赖于线程上下文。
		4. 处理信号
			在处理操作系统信号时，可能需要将 Goroutine 锁定到特定的 M，以确保信号处理程序在同一个线程上执行。
		5. 其他需要线程亲和性的情况
			某些情况下，可能需要线程亲和性（Thread Affinity），即确保某些操作在特定的线程上执行，以提高性能或满足特定的需求。在这种情况下，Go 运行时系统会将 Goroutine 锁定到特定的 M。
	*/

	// 检查当前 goroutine 是否被锁定到特定的线程（M）。gp.lockedm 是一个指向锁定线程的指针，如果不为 0，表示该 goroutine 被锁定到特定的 M。
	if gp.lockedm != 0 {
		// Hands off own p to the locked m,
		// then blocks waiting for a new p.
		// 当前处理器（P）将交给被锁定的线程（M），然后当前线程将阻塞等待一个新的处理器（P）。

		// 调用 startlockedm 函数，将当前处理器（P）交给被锁定的线程（M），并启动该线程来运行当前 goroutine。
		startlockedm(gp)
		goto top
	}

	// 4. 调用 runtime.execute 函数在当前线程 M 上运行 G
	execute(gp, inheritTime)
}

// dropg removes the association between m and the current goroutine m->curg (gp for short).
// Typically a caller sets gp's status away from Grunning and then
// immediately calls dropg to finish the job. The caller is also responsible
// for arranging that gp will be restarted using ready at an
// appropriate time. After calling dropg and arranging for gp to be
// readied later, the caller can do other work but eventually should
// call schedule to restart the scheduling of goroutines on this m.
func dropg() {
	_g_ := getg()

	setMNoWB(&_g_.m.curg.m, nil)
	setGNoWB(&_g_.m.curg, nil)
}

// checkTimers runs any timers for the P that are ready.
// If now is not 0 it is the current time.
// It returns the current time or 0 if it is not known,
// and the time when the next timer should run or 0 if there is no next timer,
// and reports whether it ran any timers.
// If the time when the next timer should run is not 0,
// it is always larger than the returned time.
// We pass now in and out to avoid extra calls of nanotime.
//go:yeswritebarrierrec
func checkTimers(pp *p, now int64) (rnow, pollUntil int64, ran bool) {
	// If there are no timers to adjust, and the first timer on
	// the heap is not yet ready to run, then there is nothing to do.
	if atomic.Load(&pp.adjustTimers) == 0 {
		next := int64(atomic.Load64(&pp.timer0When))
		if next == 0 {
			return now, 0, false
		}
		if now == 0 {
			now = nanotime()
		}
		if now < next {
			// Next timer is not ready to run.
			// But keep going if we would clear deleted timers.
			// This corresponds to the condition below where
			// we decide whether to call clearDeletedTimers.
			if pp != getg().m.p.ptr() || int(atomic.Load(&pp.deletedTimers)) <= int(atomic.Load(&pp.numTimers)/4) {
				return now, next, false
			}
		}
	}

	lock(&pp.timersLock)

	adjusttimers(pp)

	rnow = now
	if len(pp.timers) > 0 {
		if rnow == 0 {
			rnow = nanotime()
		}
		for len(pp.timers) > 0 {
			// Note that runtimer may temporarily unlock
			// pp.timersLock.
			if tw := runtimer(pp, rnow); tw != 0 {
				if tw > 0 {
					pollUntil = tw
				}
				break
			}
			ran = true
		}
	}

	// If this is the local P, and there are a lot of deleted timers,
	// clear them out. We only do this for the local P to reduce
	// lock contention on timersLock.
	if pp == getg().m.p.ptr() && int(atomic.Load(&pp.deletedTimers)) > len(pp.timers)/4 {
		clearDeletedTimers(pp)
	}

	unlock(&pp.timersLock)

	return rnow, pollUntil, ran
}

// shouldStealTimers reports whether we should try stealing the timers from p2.
// We don't steal timers from a running P that is not marked for preemption,
// on the assumption that it will run its own timers. This reduces
// contention on the timers lock.
func shouldStealTimers(p2 *p) bool {
	if p2.status != _Prunning {
		return true
	}
	mp := p2.m.ptr()
	if mp == nil || mp.locks > 0 {
		return false
	}
	gp := mp.curg
	if gp == nil || gp.atomicstatus != _Grunning || !gp.preempt {
		return false
	}
	return true
}

func parkunlock_c(gp *g, lock unsafe.Pointer) bool {
	unlock((*mutex)(lock))
	return true
}

// park continuation on g0.
func park_m(gp *g) {
	_g_ := getg()

	if trace.enabled {
		traceGoPark(_g_.m.waittraceev, _g_.m.waittraceskip)
	}

	casgstatus(gp, _Grunning, _Gwaiting)
	dropg()

	if fn := _g_.m.waitunlockf; fn != nil {
		ok := fn(gp, _g_.m.waitlock)
		_g_.m.waitunlockf = nil
		_g_.m.waitlock = nil
		if !ok {
			if trace.enabled {
				traceGoUnpark(gp, 2)
			}
			casgstatus(gp, _Gwaiting, _Grunnable)
			execute(gp, true) // Schedule it back, never returns.
		}
	}
	schedule()
}

/*
	goschedImpl 是 Go 运行时系统中的一个内部函数，用于实现 Gosched 的具体调度逻辑。
	这个函数的作用是将当前 goroutine 的状态从运行中（_Grunning）切换到可运行（_Grunnable），
	然后将其放回全局运行队列，并调用调度器选择下一个 goroutine 运行。
*/
func goschedImpl(gp *g) {
	// 读取当前 goroutine 的状态。gp 是指向当前 goroutine 的指针。
	// readgstatus 是一个内部函数，用于获取 goroutine 的状态。
	status := readgstatus(gp)
	// if status&^_Gscan != _Grunning 检查当前 goroutine 的状态是否为运行中（_Grunning）。
	// _Gscan 是一个标志位，用于标记 goroutine 是否正在被扫描（例如垃圾回收期间）。
	if status&^_Gscan != _Grunning {
		dumpgstatus(gp)
		throw("bad g status")
	}
	// casgstatus(gp, _Grunning, _Grunnable) 将当前 goroutine 的状态从运行中（_Grunning）切换到可运行（_Grunnable）。
	// casgstatus 是一个内部函数，用于原子地比较并交换 goroutine 的状态。
	casgstatus(gp, _Grunning, _Grunnable)
	// dropg() 将当前 goroutine 从当前处理器（P）中移除。
	// dropg 是一个内部函数，用于处理 goroutine 和处理器之间的关系。
	dropg()
	// lock(&sched.lock) 获取全局调度器的锁。
	lock(&sched.lock)
	// globrunqput(gp) 将当前 goroutine 放回全局运行队列。
	globrunqput(gp)
	// unlock(&sched.lock) 释放全局调度器的锁。
	unlock(&sched.lock)

	// schedule() 调用调度器，选择下一个 goroutine 运行。
	// schedule 是一个内部函数，用于实现调度逻辑。
	schedule()
}

/*
	虽然 gosched_m 的具体实现是内部的运行时细节，但其主要作用是将当前 goroutine 放回到调度队列中，并选择另一个 goroutine 运行。
	这是 Go 运行时调度器的核心功能之一，确保多个 goroutine 能够公平地共享处理器资源。

	gosched_m 在 g0 栈上执行 Gosched 操作。g0 栈是 Go 运行时的调度栈，用于执行调度相关的操作。
	这个函数的主要目的是让出当前 goroutine 的执行权，使得调度器可以选择运行其他 goroutine。
*/
// 到这里当前G已经让出执行权给 g0，加下来的逻辑是在g0中继续。
// Gosched continuation on g0.
func gosched_m(gp *g) {
	if trace.enabled {
		traceGoSched()
	}
	goschedImpl(gp)
}

// goschedguarded is a forbidden-states-avoided version of gosched_m
func goschedguarded_m(gp *g) {

	if !canPreemptM(gp.m) {
		gogo(&gp.sched) // never return
	}

	if trace.enabled {
		traceGoSched()
	}
	goschedImpl(gp)
}

func gopreempt_m(gp *g) {
	if trace.enabled {
		traceGoPreempt()
	}
	goschedImpl(gp)
}

// preemptPark parks gp and puts it in _Gpreempted.
//
//go:systemstack
func preemptPark(gp *g) {
	if trace.enabled {
		traceGoPark(traceEvGoBlock, 0)
	}
	status := readgstatus(gp)
	if status&^_Gscan != _Grunning {
		dumpgstatus(gp)
		throw("bad g status")
	}
	gp.waitreason = waitReasonPreempted
	// Transition from _Grunning to _Gscan|_Gpreempted. We can't
	// be in _Grunning when we dropg because then we'd be running
	// without an M, but the moment we're in _Gpreempted,
	// something could claim this G before we've fully cleaned it
	// up. Hence, we set the scan bit to lock down further
	// transitions until we can dropg.
	casGToPreemptScan(gp, _Grunning, _Gscan|_Gpreempted)
	dropg()
	casfrom_Gscanstatus(gp, _Gscan|_Gpreempted, _Gpreempted)
	schedule()
}

// goyield is like Gosched, but it:
// - emits a GoPreempt trace event instead of a GoSched trace event
// - puts the current G on the runq of the current P instead of the globrunq
func goyield() {
	checkTimeouts()
	mcall(goyield_m)
}

func goyield_m(gp *g) {
	if trace.enabled {
		traceGoPreempt()
	}
	pp := gp.m.p.ptr()
	casgstatus(gp, _Grunning, _Grunnable)
	dropg()
	runqput(pp, gp, false)
	schedule()
}

// Finishes execution of the current goroutine.
func goexit1() {
	if raceenabled {
		racegoend()
	}
	if trace.enabled {
		traceGoEnd()
	}
	mcall(goexit0)
}

// goexit continuation on g0.
func goexit0(gp *g) {
	_g_ := getg()

	casgstatus(gp, _Grunning, _Gdead)
	if isSystemGoroutine(gp, false) {
		atomic.Xadd(&sched.ngsys, -1)
	}
	gp.m = nil
	locked := gp.lockedm != 0
	gp.lockedm = 0
	_g_.m.lockedg = 0
	gp.preemptStop = false
	gp.paniconfault = false
	gp._defer = nil // should be true already but just in case.
	gp._panic = nil // non-nil for Goexit during panic. points at stack-allocated data.
	gp.writebuf = nil
	gp.waitreason = 0
	gp.param = nil
	gp.labels = nil
	gp.timer = nil

	if gcBlackenEnabled != 0 && gp.gcAssistBytes > 0 {
		// Flush assist credit to the global pool. This gives
		// better information to pacing if the application is
		// rapidly creating an exiting goroutines.
		scanCredit := int64(gcController.assistWorkPerByte * float64(gp.gcAssistBytes))
		atomic.Xaddint64(&gcController.bgScanCredit, scanCredit)
		gp.gcAssistBytes = 0
	}

	dropg()

	if GOARCH == "wasm" { // no threads yet on wasm
		gfput(_g_.m.p.ptr(), gp)
		schedule() // never returns
	}

	if _g_.m.lockedInt != 0 {
		print("invalid m->lockedInt = ", _g_.m.lockedInt, "\n")
		throw("internal lockOSThread error")
	}
	gfput(_g_.m.p.ptr(), gp)
	if locked {
		// The goroutine may have locked this thread because
		// it put it in an unusual kernel state. Kill it
		// rather than returning it to the thread pool.

		// Return to mstart, which will release the P and exit
		// the thread.
		if GOOS != "plan9" { // See golang.org/issue/22227.
			gogo(&_g_.m.g0.sched)
		} else {
			// Clear lockedExt on plan9 since we may end up re-using
			// this thread.
			_g_.m.lockedExt = 0
		}
	}
	schedule()
}

// save updates getg().sched to refer to pc and sp so that a following
// gogo will restore pc and sp.
//
// save must not have write barriers because invoking a write barrier
// can clobber getg().sched.
//
//go:nosplit
//go:nowritebarrierrec
func save(pc, sp uintptr) {
	_g_ := getg()

	_g_.sched.pc = pc
	_g_.sched.sp = sp
	_g_.sched.lr = 0
	_g_.sched.ret = 0
	// 将当前 G 的指针存储在调度器的 g 字段中。
	_g_.sched.g = guintptr(unsafe.Pointer(_g_))
	// We need to ensure ctxt is zero, but can't have a write
	// barrier here. However, it should always already be zero.
	// Assert that.
	if _g_.sched.ctxt != nil {
		badctxt()
	}
}

// The goroutine g is about to enter a system call.
// Record that it's not using the cpu anymore.
// This is called only from the go syscall library and cgocall,
// not from the low-level system calls used by the runtime.
//
// Entersyscall cannot split the stack: the gosave must
// make g->sched refer to the caller's stack segment, because
// entersyscall is going to return immediately after.
//
// Nothing entersyscall calls can split the stack either.
// We cannot safely move the stack during an active call to syscall,
// because we do not know which of the uintptr arguments are
// really pointers (back into the stack).
// In practice, this means that we make the fast path run through
// entersyscall doing no-split things, and the slow path has to use systemstack
// to run bigger things on the system stack.
//
// reentersyscall is the entry point used by cgo callbacks, where explicitly
// saved SP and PC are restored. This is needed when exitsyscall will be called
// from a function further up in the call stack than the parent, as g->syscallsp
// must always point to a valid stack frame. entersyscall below is the normal
// entry point for syscalls, which obtains the SP and PC from the caller.
//
// Syscall tracing:
// At the start of a syscall we emit traceGoSysCall to capture the stack trace.
// If the syscall does not block, that is it, we do not emit any other events.
// If the syscall blocks (that is, P is retaken), retaker emits traceGoSysBlock;
// when syscall returns we emit traceGoSysExit and when the goroutine starts running
// (potentially instantly, if exitsyscallfast returns true) we emit traceGoStart.
// To ensure that traceGoSysExit is emitted strictly after traceGoSysBlock,
// we remember current value of syscalltick in m (_g_.m.syscalltick = _g_.m.p.ptr().syscalltick),
// whoever emits traceGoSysBlock increments p.syscalltick afterwards;
// and we wait for the increment before emitting traceGoSysExit.
// Note that the increment is done even if tracing is not enabled,
// because tracing can be enabled in the middle of syscall. We don't want the wait to hang.
//
//go:nosplit
func reentersyscall(pc, sp uintptr) {
	_g_ := getg()

	// Disable preemption because during this function g is in Gsyscall status,
	// but can have inconsistent g->sched, do not let GC observe it.
	_g_.m.locks++

	// Entersyscall must not call any function that might split/grow the stack.
	// (See details in comment above.)
	// Catch calls that might, by replacing the stack guard with something that
	// will trip any stack check and leaving a flag to tell newstack to die.
	_g_.stackguard0 = stackPreempt
	_g_.throwsplit = true

	// Leave SP around for GC and traceback.
	save(pc, sp)
	_g_.syscallsp = sp
	_g_.syscallpc = pc
	casgstatus(_g_, _Grunning, _Gsyscall)
	if _g_.syscallsp < _g_.stack.lo || _g_.stack.hi < _g_.syscallsp {
		systemstack(func() {
			print("entersyscall inconsistent ", hex(_g_.syscallsp), " [", hex(_g_.stack.lo), ",", hex(_g_.stack.hi), "]\n")
			throw("entersyscall")
		})
	}

	if trace.enabled {
		systemstack(traceGoSysCall)
		// systemstack itself clobbers g.sched.{pc,sp} and we might
		// need them later when the G is genuinely blocked in a
		// syscall
		save(pc, sp)
	}

	if atomic.Load(&sched.sysmonwait) != 0 {
		systemstack(entersyscall_sysmon)
		save(pc, sp)
	}

	if _g_.m.p.ptr().runSafePointFn != 0 {
		// runSafePointFn may stack split if run on this stack
		systemstack(runSafePointFn)
		save(pc, sp)
	}

	_g_.m.syscalltick = _g_.m.p.ptr().syscalltick
	_g_.sysblocktraced = true
	pp := _g_.m.p.ptr()
	pp.m = 0
	_g_.m.oldp.set(pp)
	_g_.m.p = 0
	atomic.Store(&pp.status, _Psyscall)
	if sched.gcwaiting != 0 {
		systemstack(entersyscall_gcwait)
		save(pc, sp)
	}

	_g_.m.locks--
}

// Standard syscall entry used by the go syscall library and normal cgo calls.
//
// This is exported via linkname to assembly in the syscall package.
//
//go:nosplit
//go:linkname entersyscall
func entersyscall() {
	reentersyscall(getcallerpc(), getcallersp())
}

func entersyscall_sysmon() {
	lock(&sched.lock)
	if atomic.Load(&sched.sysmonwait) != 0 {
		atomic.Store(&sched.sysmonwait, 0)
		notewakeup(&sched.sysmonnote)
	}
	unlock(&sched.lock)
}

func entersyscall_gcwait() {
	_g_ := getg()
	_p_ := _g_.m.oldp.ptr()

	lock(&sched.lock)
	if sched.stopwait > 0 && atomic.Cas(&_p_.status, _Psyscall, _Pgcstop) {
		if trace.enabled {
			traceGoSysBlock(_p_)
			traceProcStop(_p_)
		}
		_p_.syscalltick++
		if sched.stopwait--; sched.stopwait == 0 {
			notewakeup(&sched.stopnote)
		}
	}
	unlock(&sched.lock)
}

// The same as entersyscall(), but with a hint that the syscall is blocking.
//go:nosplit
func entersyscallblock() {
	_g_ := getg()

	_g_.m.locks++ // see comment in entersyscall
	_g_.throwsplit = true
	_g_.stackguard0 = stackPreempt // see comment in entersyscall
	_g_.m.syscalltick = _g_.m.p.ptr().syscalltick
	_g_.sysblocktraced = true
	_g_.m.p.ptr().syscalltick++

	// Leave SP around for GC and traceback.
	pc := getcallerpc()
	sp := getcallersp()
	save(pc, sp)
	_g_.syscallsp = _g_.sched.sp
	_g_.syscallpc = _g_.sched.pc
	if _g_.syscallsp < _g_.stack.lo || _g_.stack.hi < _g_.syscallsp {
		sp1 := sp
		sp2 := _g_.sched.sp
		sp3 := _g_.syscallsp
		systemstack(func() {
			print("entersyscallblock inconsistent ", hex(sp1), " ", hex(sp2), " ", hex(sp3), " [", hex(_g_.stack.lo), ",", hex(_g_.stack.hi), "]\n")
			throw("entersyscallblock")
		})
	}
	casgstatus(_g_, _Grunning, _Gsyscall)
	if _g_.syscallsp < _g_.stack.lo || _g_.stack.hi < _g_.syscallsp {
		systemstack(func() {
			print("entersyscallblock inconsistent ", hex(sp), " ", hex(_g_.sched.sp), " ", hex(_g_.syscallsp), " [", hex(_g_.stack.lo), ",", hex(_g_.stack.hi), "]\n")
			throw("entersyscallblock")
		})
	}

	systemstack(entersyscallblock_handoff)

	// Resave for traceback during blocked call.
	save(getcallerpc(), getcallersp())

	_g_.m.locks--
}

func entersyscallblock_handoff() {
	if trace.enabled {
		traceGoSysCall()
		traceGoSysBlock(getg().m.p.ptr())
	}
	handoffp(releasep())
}

// The goroutine g exited its system call.
// Arrange for it to run on a cpu again.
// This is called only from the go syscall library, not
// from the low-level system calls used by the runtime.
//
// Write barriers are not allowed because our P may have been stolen.
//
// This is exported via linkname to assembly in the syscall package.
//
//go:nosplit
//go:nowritebarrierrec
//go:linkname exitsyscall
func exitsyscall() {
	_g_ := getg()

	_g_.m.locks++ // see comment in entersyscall
	if getcallersp() > _g_.syscallsp {
		throw("exitsyscall: syscall frame is no longer valid")
	}

	_g_.waitsince = 0
	oldp := _g_.m.oldp.ptr()
	_g_.m.oldp = 0
	if exitsyscallfast(oldp) {
		if trace.enabled {
			if oldp != _g_.m.p.ptr() || _g_.m.syscalltick != _g_.m.p.ptr().syscalltick {
				systemstack(traceGoStart)
			}
		}
		// There's a cpu for us, so we can run.
		_g_.m.p.ptr().syscalltick++
		// We need to cas the status and scan before resuming...
		casgstatus(_g_, _Gsyscall, _Grunning)

		// Garbage collector isn't running (since we are),
		// so okay to clear syscallsp.
		_g_.syscallsp = 0
		_g_.m.locks--
		if _g_.preempt {
			// restore the preemption request in case we've cleared it in newstack
			_g_.stackguard0 = stackPreempt
		} else {
			// otherwise restore the real _StackGuard, we've spoiled it in entersyscall/entersyscallblock
			_g_.stackguard0 = _g_.stack.lo + _StackGuard
		}
		_g_.throwsplit = false

		if sched.disable.user && !schedEnabled(_g_) {
			// Scheduling of this goroutine is disabled.
			Gosched()
		}

		return
	}

	_g_.sysexitticks = 0
	if trace.enabled {
		// Wait till traceGoSysBlock event is emitted.
		// This ensures consistency of the trace (the goroutine is started after it is blocked).
		for oldp != nil && oldp.syscalltick == _g_.m.syscalltick {
			osyield()
		}
		// We can't trace syscall exit right now because we don't have a P.
		// Tracing code can invoke write barriers that cannot run without a P.
		// So instead we remember the syscall exit time and emit the event
		// in execute when we have a P.
		_g_.sysexitticks = cputicks()
	}

	_g_.m.locks--

	// Call the scheduler.
	mcall(exitsyscall0)

	// Scheduler returned, so we're allowed to run now.
	// Delete the syscallsp information that we left for
	// the garbage collector during the system call.
	// Must wait until now because until gosched returns
	// we don't know for sure that the garbage collector
	// is not running.
	_g_.syscallsp = 0
	_g_.m.p.ptr().syscalltick++
	_g_.throwsplit = false
}

//go:nosplit
func exitsyscallfast(oldp *p) bool {
	_g_ := getg()

	// Freezetheworld sets stopwait but does not retake P's.
	if sched.stopwait == freezeStopWait {
		return false
	}

	// Try to re-acquire the last P.
	if oldp != nil && oldp.status == _Psyscall && atomic.Cas(&oldp.status, _Psyscall, _Pidle) {
		// There's a cpu for us, so we can run.
		wirep(oldp)
		exitsyscallfast_reacquired()
		return true
	}

	// Try to get any other idle P.
	if sched.pidle != 0 {
		var ok bool
		systemstack(func() {
			ok = exitsyscallfast_pidle()
			if ok && trace.enabled {
				if oldp != nil {
					// Wait till traceGoSysBlock event is emitted.
					// This ensures consistency of the trace (the goroutine is started after it is blocked).
					for oldp.syscalltick == _g_.m.syscalltick {
						osyield()
					}
				}
				traceGoSysExit(0)
			}
		})
		if ok {
			return true
		}
	}
	return false
}

// exitsyscallfast_reacquired is the exitsyscall path on which this G
// has successfully reacquired the P it was running on before the
// syscall.
//
//go:nosplit
func exitsyscallfast_reacquired() {
	_g_ := getg()
	if _g_.m.syscalltick != _g_.m.p.ptr().syscalltick {
		if trace.enabled {
			// The p was retaken and then enter into syscall again (since _g_.m.syscalltick has changed).
			// traceGoSysBlock for this syscall was already emitted,
			// but here we effectively retake the p from the new syscall running on the same p.
			systemstack(func() {
				// Denote blocking of the new syscall.
				traceGoSysBlock(_g_.m.p.ptr())
				// Denote completion of the current syscall.
				traceGoSysExit(0)
			})
		}
		_g_.m.p.ptr().syscalltick++
	}
}

func exitsyscallfast_pidle() bool {
	lock(&sched.lock)
	_p_ := pidleget()
	if _p_ != nil && atomic.Load(&sched.sysmonwait) != 0 {
		atomic.Store(&sched.sysmonwait, 0)
		notewakeup(&sched.sysmonnote)
	}
	unlock(&sched.lock)
	if _p_ != nil {
		acquirep(_p_)
		return true
	}
	return false
}

// exitsyscall slow path on g0.
// Failed to acquire P, enqueue gp as runnable.
//
//go:nowritebarrierrec
func exitsyscall0(gp *g) {
	_g_ := getg()

	casgstatus(gp, _Gsyscall, _Grunnable)
	dropg()
	lock(&sched.lock)
	var _p_ *p
	if schedEnabled(_g_) {
		_p_ = pidleget()
	}
	if _p_ == nil {
		globrunqput(gp)
	} else if atomic.Load(&sched.sysmonwait) != 0 {
		atomic.Store(&sched.sysmonwait, 0)
		notewakeup(&sched.sysmonnote)
	}
	unlock(&sched.lock)
	if _p_ != nil {
		acquirep(_p_)
		execute(gp, false) // Never returns.
	}
	if _g_.m.lockedg != 0 {
		// Wait until another thread schedules gp and so m again.
		stoplockedm()
		execute(gp, false) // Never returns.
	}
	stopm()
	schedule() // Never returns.
}

func beforefork() {
	gp := getg().m.curg

	// Block signals during a fork, so that the child does not run
	// a signal handler before exec if a signal is sent to the process
	// group. See issue #18600.
	gp.m.locks++
	msigsave(gp.m)
	sigblock()

	// This function is called before fork in syscall package.
	// Code between fork and exec must not allocate memory nor even try to grow stack.
	// Here we spoil g->_StackGuard to reliably detect any attempts to grow stack.
	// runtime_AfterFork will undo this in parent process, but not in child.
	gp.stackguard0 = stackFork
}

// Called from syscall package before fork.
//go:linkname syscall_runtime_BeforeFork syscall.runtime_BeforeFork
//go:nosplit
func syscall_runtime_BeforeFork() {
	systemstack(beforefork)
}

func afterfork() {
	gp := getg().m.curg

	// See the comments in beforefork.
	gp.stackguard0 = gp.stack.lo + _StackGuard

	msigrestore(gp.m.sigmask)

	gp.m.locks--
}

// Called from syscall package after fork in parent.
//go:linkname syscall_runtime_AfterFork syscall.runtime_AfterFork
//go:nosplit
func syscall_runtime_AfterFork() {
	systemstack(afterfork)
}

// inForkedChild is true while manipulating signals in the child process.
// This is used to avoid calling libc functions in case we are using vfork.
var inForkedChild bool

// Called from syscall package after fork in child.
// It resets non-sigignored signals to the default handler, and
// restores the signal mask in preparation for the exec.
//
// Because this might be called during a vfork, and therefore may be
// temporarily sharing address space with the parent process, this must
// not change any global variables or calling into C code that may do so.
//
//go:linkname syscall_runtime_AfterForkInChild syscall.runtime_AfterForkInChild
//go:nosplit
//go:nowritebarrierrec
func syscall_runtime_AfterForkInChild() {
	// It's OK to change the global variable inForkedChild here
	// because we are going to change it back. There is no race here,
	// because if we are sharing address space with the parent process,
	// then the parent process can not be running concurrently.
	inForkedChild = true

	clearSignalHandlers()

	// When we are the child we are the only thread running,
	// so we know that nothing else has changed gp.m.sigmask.
	msigrestore(getg().m.sigmask)

	inForkedChild = false
}

// Called from syscall package before Exec.
//go:linkname syscall_runtime_BeforeExec syscall.runtime_BeforeExec
func syscall_runtime_BeforeExec() {
	// Prevent thread creation during exec.
	execLock.lock()
}

// Called from syscall package after Exec.
//go:linkname syscall_runtime_AfterExec syscall.runtime_AfterExec
func syscall_runtime_AfterExec() {
	execLock.unlock()
}

// Allocate a new g, with a stack big enough for stacksize bytes.
func malg(stacksize int32) *g {
	// New一个 g struct的对象
	newg := new(g)
	if stacksize >= 0 {
		// round2 是一个函数，用于将栈大小向上对齐到 2 的幂次方。
		// _StackSystem 表示在特定操作系统上为每个栈添加的额外字节数。这些额外的字节用于操作系统特定的目的，例如信号处理。
		// 这行代码的作用是计算总的栈大小，并将其向上对齐到 2 的幂次方。
		stacksize = round2(_StackSystem + stacksize)
		// 这行代码的作用是在系统栈上分配 stacksize 大小的栈空间，并将其赋值给 newg.stack。
		systemstack(func() {
			// 调用 runtime.stackalloc 分配一个大小足够栈内存空间，
			// 根据线程缓存和申请栈的大小，该函数会通过三种不同的方法分配栈空间
			// 1.如果栈空间较小，使用全局栈缓存或者线程缓存上固定大小的空闲链表分配内存；
			// 2.如果栈空间较大，从全局的大栈缓存 runtime.stackLarge 中获取内存空间；
			// 3.如果栈空间较大并且 runtime.stackLarge 空间不足，在堆上申请一片大小足够内存空间；
			newg.stack = stackalloc(uint32(stacksize))
		})
		// 这行代码的作用是设置 newg 的栈保护边界 stackguard0，用于检测栈溢出。
		newg.stackguard0 = newg.stack.lo + _StackGuard
		// ^uintptr(0) 是一个特殊的值，表示无穷大。
		newg.stackguard1 = ^uintptr(0)
		// Clear the bottom word of the stack. We record g
		// there on gsignal stack during VDSO on ARM and ARM64.
		*(*uintptr)(unsafe.Pointer(newg.stack.lo)) = 0
	}
	return newg
}

// Create a new g running fn with siz bytes of arguments.
// Put it on the queue of g's waiting to run.
// The compiler turns a go statement into a call to this.
//
// The stack layout of this call is unusual: it assumes that the
// arguments to pass to fn are on the stack sequentially immediately
// after &fn. Hence, they are logically part of newproc's argument
// frame, even though they don't appear in its signature (and can't
// because their types differ between call sites).
//
// This must be nosplit because this stack layout means there are
// untyped arguments in newproc's argument frame. Stack copies won't
// be able to adjust them and stack splits won't be able to copy them.
//
/*
	这段 Go 代码定义了一个名为 newproc 的函数，用于创建一个新的 goroutine 并将其放入等待运行的 goroutine 队列中。
	编译器会将 go 语句转换为对这个函数的调用。

	1. 创建一个新的 goroutine 运行 fn
	2. 并将其放入等待运行的 goroutine 队列中。
	3. 编译器会将 go 语句转换为对 newproc 函数的调用。
*/
//go:nosplit
func newproc(siz int32, fn *funcval) {
	argp := add(unsafe.Pointer(&fn), sys.PtrSize)
	// 获取指向当前G的指针
	gp := getg()
	// 获取调用方的程序计数器
	pc := getcallerpc()
	// 将传进来的函数运行在系统栈
	systemstack(func() {
		// 在系统栈上，调用 newproc1 函数创建一个新的 goroutine，并将其存储在 newg 变量中。
		// newproc1 函数负责分配和初始化新的 goroutine。
		newg := newproc1(fn, argp, siz, gp, pc)

		// 获取当前的 P（处理器）的指针，并将其存储在 _p_ 变量中。
		_p_ := getg().m.p.ptr()
		// 将新的 goroutine 放入 P 的运行队列中，等待调度执行。
		// runqput 函数负责将 goroutine 放入运行队列。如果next参数为true，则放入p的runnext中。
		runqput(_p_, newg, true)

		// 如果主 goroutine 已经启动，则调用 wakep 函数唤醒一个处理器，以便尽快调度新的 goroutine。
		// mainStarted 是一个全局变量，指示主 goroutine 是否已经启动。
		if mainStarted {
			// 注意：wakep函数中会根据情况，启动新的M
			wakep()
		}
	})
}

/*
	创建一个新的 goroutine，初始状态为 _Grunnable，从 fn 函数开始执行，参数从 argp 开始，参数大小为 narg 字节。
	callerpc 是创建这个 goroutine 的 go 语句的地址。
	调用者负责将新的 goroutine 添加到调度器中。
	这个函数必须在系统栈上运行，因为它是 newproc 函数的延续，而 newproc 函数不能拆分栈。
*/
// Create a new g in state _Grunnable, starting at fn, with narg bytes
// of arguments starting at argp. callerpc is the address of the go
// statement that created this. The caller is responsible for adding
// the new g to the scheduler.
//
// This must run on the system stack because it's the continuation of
// newproc, which cannot split the stack.
//
//go:systemstack
func newproc1(fn *funcval, argp unsafe.Pointer, narg int32, callergp *g, callerpc uintptr) *g {
	/*
		这里注意：
		因为newproc1切换到了系统栈上执行，所以会切换到g0，只有g0才能在系统栈上执行。
		所以 _g_ 应该是g0，而 callergp 则是调用newproc1函数的G。两者是不一样的。

		只有g0才能在系统栈上执行，所以：
		在使用 //go:systemstack 指令的函数中，getg 返回的通常是 g0。
	*/
	_g_ := getg()

	if fn == nil {
		_g_.m.throwing = -1 // do not dump full stacks
		throw("go of nil func value")
	}
	// 获取当前G关联的m，这里会给m上锁，防止m被抢占或调用到其他G
	acquirem() // disable preemption because it can be holding p in a local var
	// 这行代码的作用是将 siz 向上对齐到 8 字节的边界。
	// 具体来说，它先将 siz 加 7，然后使用按位与非操作 &^ 清除最低的 3 位（即 &^ 7），从而实现 8 字节对齐。这是为了确保参数在内存中的对齐要求，避免潜在的性能问题或错误。
	siz := narg
	siz = (siz + 7) &^ 7

	// We could allocate a larger initial stack if necessary.
	// Not worth it: this is almost always an error.
	// 4*sizeof(uintreg): extra space added below
	// sizeof(uintreg): caller's LR (arm) or return address (x86, in gostartcall).
	// _StackMin 是 Go 运行时中定义的最小栈大小常量。
	// 4*sys.RegSize 和 sys.RegSize 是额外的空间，用于存储一些运行时信息和返回地址。
	if siz >= _StackMin-4*sys.RegSize-sys.RegSize {
		throw("newproc: function arguments too large for new goroutine")
	}

	_p_ := _g_.m.p.ptr()
	// 从当前p的空闲g列表中，获取一个g。这些g都是可以重用的g。
	newg := gfget(_p_)
	if newg == nil {
		// 用于分配一个新的 goroutine 结构体 g，并为其分配一个足够大的栈空间。
		newg = malg(_StackMin)
		// 将g的状态从 _Gidle 更改为 _Gdead
		casgstatus(newg, _Gidle, _Gdead)
		// 将g添加到全局队列
		allgadd(newg) // publishes with a g->status of Gdead so GC scanner doesn't look at uninitialized stack.
	}
	if newg.stack.hi == 0 {
		throw("newproc1: newg missing stack")
	}

	if readgstatus(newg) != _Gdead {
		throw("newproc1: new g is not Gdead")
	}

	totalSize := 4*sys.RegSize + uintptr(siz) + sys.MinFrameSize // extra space in case of reads slightly beyond frame
	totalSize += -totalSize & (sys.SpAlign - 1)                  // align to spAlign
	sp := newg.stack.hi - totalSize
	spArg := sp
	if usesLR {
		// caller's LR
		*(*uintptr)(unsafe.Pointer(sp)) = 0
		prepGoExitFrame(sp)
		spArg += sys.MinFrameSize
	}
	if narg > 0 {
		memmove(unsafe.Pointer(spArg), argp, uintptr(narg))
		// This is a stack-to-stack copy. If write barriers
		// are enabled and the source stack is grey (the
		// destination is always black), then perform a
		// barrier copy. We do this *after* the memmove
		// because the destination stack may have garbage on
		// it.
		if writeBarrier.needed && !_g_.m.curg.gcscandone {
			f := findfunc(fn.fn)
			stkmap := (*stackmap)(funcdata(f, _FUNCDATA_ArgsPointerMaps))
			if stkmap.nbit > 0 {
				// We're in the prologue, so it's always stack map index 0.
				bv := stackmapdata(stkmap, 0)
				bulkBarrierBitmap(spArg, spArg, uintptr(bv.n)*sys.PtrSize, 0, bv.bytedata)
			}
		}
	}

	memclrNoHeapPointers(unsafe.Pointer(&newg.sched), unsafe.Sizeof(newg.sched))
	newg.sched.sp = sp
	newg.stktopsp = sp
	newg.sched.pc = funcPC(goexit) + sys.PCQuantum // +PCQuantum so that previous instruction is in same function
	newg.sched.g = guintptr(unsafe.Pointer(newg))
	gostartcallfn(&newg.sched, fn)
	newg.gopc = callerpc
	newg.ancestors = saveAncestors(callergp)
	newg.startpc = fn.fn
	if _g_.m.curg != nil {
		newg.labels = _g_.m.curg.labels
	}
	if isSystemGoroutine(newg, false) {
		atomic.Xadd(&sched.ngsys, +1)
	}
	// 将New g 的状态更改为 _Grunnable
	casgstatus(newg, _Gdead, _Grunnable)

	if _p_.goidcache == _p_.goidcacheend {
		// Sched.goidgen is the last allocated id,
		// this batch must be [sched.goidgen+1, sched.goidgen+GoidCacheBatch].
		// At startup sched.goidgen=0, so main goroutine receives goid=1.
		_p_.goidcache = atomic.Xadd64(&sched.goidgen, _GoidCacheBatch)
		_p_.goidcache -= _GoidCacheBatch - 1
		_p_.goidcacheend = _p_.goidcache + _GoidCacheBatch
	}
	newg.goid = int64(_p_.goidcache)
	_p_.goidcache++
	if raceenabled {
		newg.racectx = racegostart(callerpc)
	}
	if trace.enabled {
		traceGoCreate(newg, newg.startpc)
	}
	releasem(_g_.m)

	return newg
}

// saveAncestors copies previous ancestors of the given caller g and
// includes infor for the current caller into a new set of tracebacks for
// a g being created.
func saveAncestors(callergp *g) *[]ancestorInfo {
	// Copy all prior info, except for the root goroutine (goid 0).
	if debug.tracebackancestors <= 0 || callergp.goid == 0 {
		return nil
	}
	var callerAncestors []ancestorInfo
	if callergp.ancestors != nil {
		callerAncestors = *callergp.ancestors
	}
	n := int32(len(callerAncestors)) + 1
	if n > debug.tracebackancestors {
		n = debug.tracebackancestors
	}
	ancestors := make([]ancestorInfo, n)
	copy(ancestors[1:], callerAncestors)

	var pcs [_TracebackMaxFrames]uintptr
	npcs := gcallers(callergp, 0, pcs[:])
	ipcs := make([]uintptr, npcs)
	copy(ipcs, pcs[:])
	ancestors[0] = ancestorInfo{
		pcs:  ipcs,
		goid: callergp.goid,
		gopc: callergp.gopc,
	}

	ancestorsp := new([]ancestorInfo)
	*ancestorsp = ancestors
	return ancestorsp
}

// Put on gfree list.
// If local list is too long, transfer a batch to the global list.
func gfput(_p_ *p, gp *g) {
	if readgstatus(gp) != _Gdead {
		throw("gfput: bad status (not Gdead)")
	}

	stksize := gp.stack.hi - gp.stack.lo

	if stksize != _FixedStack {
		// non-standard stack size - free it.
		stackfree(gp.stack)
		gp.stack.lo = 0
		gp.stack.hi = 0
		gp.stackguard0 = 0
	}

	_p_.gFree.push(gp)
	_p_.gFree.n++
	if _p_.gFree.n >= 64 {
		lock(&sched.gFree.lock)
		for _p_.gFree.n >= 32 {
			_p_.gFree.n--
			gp = _p_.gFree.pop()
			if gp.stack.lo == 0 {
				sched.gFree.noStack.push(gp)
			} else {
				sched.gFree.stack.push(gp)
			}
			sched.gFree.n++
		}
		unlock(&sched.gFree.lock)
	}
}

// Get from gfree list.
// If local list is empty, grab a batch from global list.
func gfget(_p_ *p) *g {
retry:
	if _p_.gFree.empty() && (!sched.gFree.stack.empty() || !sched.gFree.noStack.empty()) {
		lock(&sched.gFree.lock)
		// Move a batch of free Gs to the P.
		for _p_.gFree.n < 32 {
			// Prefer Gs with stacks.
			gp := sched.gFree.stack.pop()
			if gp == nil {
				gp = sched.gFree.noStack.pop()
				if gp == nil {
					break
				}
			}
			sched.gFree.n--
			_p_.gFree.push(gp)
			_p_.gFree.n++
		}
		unlock(&sched.gFree.lock)
		goto retry
	}
	gp := _p_.gFree.pop()
	if gp == nil {
		return nil
	}
	_p_.gFree.n--
	if gp.stack.lo == 0 {
		// Stack was deallocated in gfput. Allocate a new one.
		systemstack(func() {
			gp.stack = stackalloc(_FixedStack)
		})
		gp.stackguard0 = gp.stack.lo + _StackGuard
	} else {
		if raceenabled {
			racemalloc(unsafe.Pointer(gp.stack.lo), gp.stack.hi-gp.stack.lo)
		}
		if msanenabled {
			msanmalloc(unsafe.Pointer(gp.stack.lo), gp.stack.hi-gp.stack.lo)
		}
	}
	return gp
}

// Purge all cached G's from gfree list to the global list.
func gfpurge(_p_ *p) {
	lock(&sched.gFree.lock)
	for !_p_.gFree.empty() {
		gp := _p_.gFree.pop()
		_p_.gFree.n--
		if gp.stack.lo == 0 {
			sched.gFree.noStack.push(gp)
		} else {
			sched.gFree.stack.push(gp)
		}
		sched.gFree.n++
	}
	unlock(&sched.gFree.lock)
}

// Breakpoint executes a breakpoint trap.
func Breakpoint() {
	breakpoint()
}

// dolockOSThread is called by LockOSThread and lockOSThread below
// after they modify m.locked. Do not allow preemption during this call,
// or else the m might be different in this function than in the caller.
//go:nosplit
func dolockOSThread() {
	if GOARCH == "wasm" {
		return // no threads on wasm yet
	}
	_g_ := getg()
	_g_.m.lockedg.set(_g_)
	_g_.lockedm.set(_g_.m)
}

//go:nosplit

// LockOSThread wires the calling goroutine to its current operating system thread.
// The calling goroutine will always execute in that thread,
// and no other goroutine will execute in it,
// until the calling goroutine has made as many calls to
// UnlockOSThread as to LockOSThread.
// If the calling goroutine exits without unlocking the thread,
// the thread will be terminated.
//
// All init functions are run on the startup thread. Calling LockOSThread
// from an init function will cause the main function to be invoked on
// that thread.
//
// A goroutine should call LockOSThread before calling OS services or
// non-Go library functions that depend on per-thread state.
func LockOSThread() {
	if atomic.Load(&newmHandoff.haveTemplateThread) == 0 && GOOS != "plan9" {
		// If we need to start a new thread from the locked
		// thread, we need the template thread. Start it now
		// while we're in a known-good state.
		startTemplateThread()
	}
	_g_ := getg()
	_g_.m.lockedExt++
	if _g_.m.lockedExt == 0 {
		_g_.m.lockedExt--
		panic("LockOSThread nesting overflow")
	}
	dolockOSThread()
}

//go:nosplit
func lockOSThread() {
	getg().m.lockedInt++
	dolockOSThread()
}

// dounlockOSThread is called by UnlockOSThread and unlockOSThread below
// after they update m->locked. Do not allow preemption during this call,
// or else the m might be in different in this function than in the caller.
//go:nosplit
func dounlockOSThread() {
	if GOARCH == "wasm" {
		return // no threads on wasm yet
	}
	_g_ := getg()
	if _g_.m.lockedInt != 0 || _g_.m.lockedExt != 0 {
		return
	}
	_g_.m.lockedg = 0
	_g_.lockedm = 0
}

//go:nosplit

// UnlockOSThread undoes an earlier call to LockOSThread.
// If this drops the number of active LockOSThread calls on the
// calling goroutine to zero, it unwires the calling goroutine from
// its fixed operating system thread.
// If there are no active LockOSThread calls, this is a no-op.
//
// Before calling UnlockOSThread, the caller must ensure that the OS
// thread is suitable for running other goroutines. If the caller made
// any permanent changes to the state of the thread that would affect
// other goroutines, it should not call this function and thus leave
// the goroutine locked to the OS thread until the goroutine (and
// hence the thread) exits.
func UnlockOSThread() {
	_g_ := getg()
	if _g_.m.lockedExt == 0 {
		return
	}
	_g_.m.lockedExt--
	dounlockOSThread()
}

//go:nosplit
func unlockOSThread() {
	_g_ := getg()
	if _g_.m.lockedInt == 0 {
		systemstack(badunlockosthread)
	}
	_g_.m.lockedInt--
	dounlockOSThread()
}

func badunlockosthread() {
	throw("runtime: internal error: misuse of lockOSThread/unlockOSThread")
}

func gcount() int32 {
	n := int32(allglen) - sched.gFree.n - int32(atomic.Load(&sched.ngsys))
	for _, _p_ := range allp {
		n -= _p_.gFree.n
	}

	// All these variables can be changed concurrently, so the result can be inconsistent.
	// But at least the current goroutine is running.
	if n < 1 {
		n = 1
	}
	return n
}

func mcount() int32 {
	return int32(sched.mnext - sched.nmfreed)
}

var prof struct {
	signalLock uint32
	hz         int32
}

func _System()                    { _System() }
func _ExternalCode()              { _ExternalCode() }
func _LostExternalCode()          { _LostExternalCode() }
func _GC()                        { _GC() }
func _LostSIGPROFDuringAtomic64() { _LostSIGPROFDuringAtomic64() }
func _VDSO()                      { _VDSO() }

// Called if we receive a SIGPROF signal.
// Called by the signal handler, may run during STW.
//go:nowritebarrierrec
func sigprof(pc, sp, lr uintptr, gp *g, mp *m) {
	if prof.hz == 0 {
		return
	}

	// On mips{,le}, 64bit atomics are emulated with spinlocks, in
	// runtime/internal/atomic. If SIGPROF arrives while the program is inside
	// the critical section, it creates a deadlock (when writing the sample).
	// As a workaround, create a counter of SIGPROFs while in critical section
	// to store the count, and pass it to sigprof.add() later when SIGPROF is
	// received from somewhere else (with _LostSIGPROFDuringAtomic64 as pc).
	if GOARCH == "mips" || GOARCH == "mipsle" || GOARCH == "arm" {
		if f := findfunc(pc); f.valid() {
			if hasPrefix(funcname(f), "runtime/internal/atomic") {
				cpuprof.lostAtomic++
				return
			}
		}
	}

	// Profiling runs concurrently with GC, so it must not allocate.
	// Set a trap in case the code does allocate.
	// Note that on windows, one thread takes profiles of all the
	// other threads, so mp is usually not getg().m.
	// In fact mp may not even be stopped.
	// See golang.org/issue/17165.
	getg().m.mallocing++

	// Define that a "user g" is a user-created goroutine, and a "system g"
	// is one that is m->g0 or m->gsignal.
	//
	// We might be interrupted for profiling halfway through a
	// goroutine switch. The switch involves updating three (or four) values:
	// g, PC, SP, and (on arm) LR. The PC must be the last to be updated,
	// because once it gets updated the new g is running.
	//
	// When switching from a user g to a system g, LR is not considered live,
	// so the update only affects g, SP, and PC. Since PC must be last, there
	// the possible partial transitions in ordinary execution are (1) g alone is updated,
	// (2) both g and SP are updated, and (3) SP alone is updated.
	// If SP or g alone is updated, we can detect the partial transition by checking
	// whether the SP is within g's stack bounds. (We could also require that SP
	// be changed only after g, but the stack bounds check is needed by other
	// cases, so there is no need to impose an additional requirement.)
	//
	// There is one exceptional transition to a system g, not in ordinary execution.
	// When a signal arrives, the operating system starts the signal handler running
	// with an updated PC and SP. The g is updated last, at the beginning of the
	// handler. There are two reasons this is okay. First, until g is updated the
	// g and SP do not match, so the stack bounds check detects the partial transition.
	// Second, signal handlers currently run with signals disabled, so a profiling
	// signal cannot arrive during the handler.
	//
	// When switching from a system g to a user g, there are three possibilities.
	//
	// First, it may be that the g switch has no PC update, because the SP
	// either corresponds to a user g throughout (as in asmcgocall)
	// or because it has been arranged to look like a user g frame
	// (as in cgocallback_gofunc). In this case, since the entire
	// transition is a g+SP update, a partial transition updating just one of
	// those will be detected by the stack bounds check.
	//
	// Second, when returning from a signal handler, the PC and SP updates
	// are performed by the operating system in an atomic update, so the g
	// update must be done before them. The stack bounds check detects
	// the partial transition here, and (again) signal handlers run with signals
	// disabled, so a profiling signal cannot arrive then anyway.
	//
	// Third, the common case: it may be that the switch updates g, SP, and PC
	// separately. If the PC is within any of the functions that does this,
	// we don't ask for a traceback. C.F. the function setsSP for more about this.
	//
	// There is another apparently viable approach, recorded here in case
	// the "PC within setsSP function" check turns out not to be usable.
	// It would be possible to delay the update of either g or SP until immediately
	// before the PC update instruction. Then, because of the stack bounds check,
	// the only problematic interrupt point is just before that PC update instruction,
	// and the sigprof handler can detect that instruction and simulate stepping past
	// it in order to reach a consistent state. On ARM, the update of g must be made
	// in two places (in R10 and also in a TLS slot), so the delayed update would
	// need to be the SP update. The sigprof handler must read the instruction at
	// the current PC and if it was the known instruction (for example, JMP BX or
	// MOV R2, PC), use that other register in place of the PC value.
	// The biggest drawback to this solution is that it requires that we can tell
	// whether it's safe to read from the memory pointed at by PC.
	// In a correct program, we can test PC == nil and otherwise read,
	// but if a profiling signal happens at the instant that a program executes
	// a bad jump (before the program manages to handle the resulting fault)
	// the profiling handler could fault trying to read nonexistent memory.
	//
	// To recap, there are no constraints on the assembly being used for the
	// transition. We simply require that g and SP match and that the PC is not
	// in gogo.
	traceback := true
	if gp == nil || sp < gp.stack.lo || gp.stack.hi < sp || setsSP(pc) || (mp != nil && mp.vdsoSP != 0) {
		traceback = false
	}
	var stk [maxCPUProfStack]uintptr
	n := 0
	if mp.ncgo > 0 && mp.curg != nil && mp.curg.syscallpc != 0 && mp.curg.syscallsp != 0 {
		cgoOff := 0
		// Check cgoCallersUse to make sure that we are not
		// interrupting other code that is fiddling with
		// cgoCallers.  We are running in a signal handler
		// with all signals blocked, so we don't have to worry
		// about any other code interrupting us.
		if atomic.Load(&mp.cgoCallersUse) == 0 && mp.cgoCallers != nil && mp.cgoCallers[0] != 0 {
			for cgoOff < len(mp.cgoCallers) && mp.cgoCallers[cgoOff] != 0 {
				cgoOff++
			}
			copy(stk[:], mp.cgoCallers[:cgoOff])
			mp.cgoCallers[0] = 0
		}

		// Collect Go stack that leads to the cgo call.
		n = gentraceback(mp.curg.syscallpc, mp.curg.syscallsp, 0, mp.curg, 0, &stk[cgoOff], len(stk)-cgoOff, nil, nil, 0)
		if n > 0 {
			n += cgoOff
		}
	} else if traceback {
		n = gentraceback(pc, sp, lr, gp, 0, &stk[0], len(stk), nil, nil, _TraceTrap|_TraceJumpStack)
	}

	if n <= 0 {
		// Normal traceback is impossible or has failed.
		// See if it falls into several common cases.
		n = 0
		if (GOOS == "windows" || GOOS == "solaris" || GOOS == "illumos" || GOOS == "darwin" || GOOS == "aix") && mp.libcallg != 0 && mp.libcallpc != 0 && mp.libcallsp != 0 {
			// Libcall, i.e. runtime syscall on windows.
			// Collect Go stack that leads to the call.
			n = gentraceback(mp.libcallpc, mp.libcallsp, 0, mp.libcallg.ptr(), 0, &stk[0], len(stk), nil, nil, 0)
		}
		if n == 0 && mp != nil && mp.vdsoSP != 0 {
			n = gentraceback(mp.vdsoPC, mp.vdsoSP, 0, gp, 0, &stk[0], len(stk), nil, nil, _TraceTrap|_TraceJumpStack)
		}
		if n == 0 {
			// If all of the above has failed, account it against abstract "System" or "GC".
			n = 2
			if inVDSOPage(pc) {
				pc = funcPC(_VDSO) + sys.PCQuantum
			} else if pc > firstmoduledata.etext {
				// "ExternalCode" is better than "etext".
				pc = funcPC(_ExternalCode) + sys.PCQuantum
			}
			stk[0] = pc
			if mp.preemptoff != "" {
				stk[1] = funcPC(_GC) + sys.PCQuantum
			} else {
				stk[1] = funcPC(_System) + sys.PCQuantum
			}
		}
	}

	if prof.hz != 0 {
		cpuprof.add(gp, stk[:n])
	}
	getg().m.mallocing--
}

// If the signal handler receives a SIGPROF signal on a non-Go thread,
// it tries to collect a traceback into sigprofCallers.
// sigprofCallersUse is set to non-zero while sigprofCallers holds a traceback.
var sigprofCallers cgoCallers
var sigprofCallersUse uint32

// sigprofNonGo is called if we receive a SIGPROF signal on a non-Go thread,
// and the signal handler collected a stack trace in sigprofCallers.
// When this is called, sigprofCallersUse will be non-zero.
// g is nil, and what we can do is very limited.
//go:nosplit
//go:nowritebarrierrec
func sigprofNonGo() {
	if prof.hz != 0 {
		n := 0
		for n < len(sigprofCallers) && sigprofCallers[n] != 0 {
			n++
		}
		cpuprof.addNonGo(sigprofCallers[:n])
	}

	atomic.Store(&sigprofCallersUse, 0)
}

// sigprofNonGoPC is called when a profiling signal arrived on a
// non-Go thread and we have a single PC value, not a stack trace.
// g is nil, and what we can do is very limited.
//go:nosplit
//go:nowritebarrierrec
func sigprofNonGoPC(pc uintptr) {
	if prof.hz != 0 {
		stk := []uintptr{
			pc,
			funcPC(_ExternalCode) + sys.PCQuantum,
		}
		cpuprof.addNonGo(stk)
	}
}

// Reports whether a function will set the SP
// to an absolute value. Important that
// we don't traceback when these are at the bottom
// of the stack since we can't be sure that we will
// find the caller.
//
// If the function is not on the bottom of the stack
// we assume that it will have set it up so that traceback will be consistent,
// either by being a traceback terminating function
// or putting one on the stack at the right offset.
func setsSP(pc uintptr) bool {
	f := findfunc(pc)
	if !f.valid() {
		// couldn't find the function for this PC,
		// so assume the worst and stop traceback
		return true
	}
	switch f.funcID {
	case funcID_gogo, funcID_systemstack, funcID_mcall, funcID_morestack:
		return true
	}
	return false
}

// setcpuprofilerate sets the CPU profiling rate to hz times per second.
// If hz <= 0, setcpuprofilerate turns off CPU profiling.
func setcpuprofilerate(hz int32) {
	// Force sane arguments.
	if hz < 0 {
		hz = 0
	}

	// Disable preemption, otherwise we can be rescheduled to another thread
	// that has profiling enabled.
	_g_ := getg()
	_g_.m.locks++

	// Stop profiler on this thread so that it is safe to lock prof.
	// if a profiling signal came in while we had prof locked,
	// it would deadlock.
	setThreadCPUProfiler(0)

	for !atomic.Cas(&prof.signalLock, 0, 1) {
		osyield()
	}
	if prof.hz != hz {
		setProcessCPUProfiler(hz)
		prof.hz = hz
	}
	atomic.Store(&prof.signalLock, 0)

	lock(&sched.lock)
	sched.profilehz = hz
	unlock(&sched.lock)

	if hz != 0 {
		setThreadCPUProfiler(hz)
	}

	_g_.m.locks--
}

// init initializes pp, which may be a freshly allocated p or a
// previously destroyed p, and transitions it to status _Pgcstop.
func (pp *p) init(id int32) {
	pp.id = id
	pp.status = _Pgcstop
	pp.sudogcache = pp.sudogbuf[:0]
	for i := range pp.deferpool {
		pp.deferpool[i] = pp.deferpoolbuf[i][:0]
	}
	pp.wbBuf.reset()
	// 为P分配mcache
	if pp.mcache == nil {
		if id == 0 {
			if mcache0 == nil {
				throw("missing mcache?")
			}
			// Use the bootstrap mcache0. Only one P will get
			// mcache0: the one with ID 0.
			pp.mcache = mcache0
		} else {
			// 创建 cache
			pp.mcache = allocmcache()
		}
	}
	if raceenabled && pp.raceprocctx == 0 {
		if id == 0 {
			pp.raceprocctx = raceprocctx0
			raceprocctx0 = 0 // bootstrap
		} else {
			pp.raceprocctx = raceproccreate()
		}
	}
	lockInit(&pp.timersLock, lockRankTimers)
}

// destroy releases all of the resources associated with pp and
// transitions it to status _Pdead.
//
// sched.lock must be held and the world must be stopped.
func (pp *p) destroy() {
	// Move all runnable goroutines to the global queue
	for pp.runqhead != pp.runqtail {
		// Pop from tail of local queue
		pp.runqtail--
		gp := pp.runq[pp.runqtail%uint32(len(pp.runq))].ptr()
		// Push onto head of global queue
		globrunqputhead(gp)
	}
	if pp.runnext != 0 {
		globrunqputhead(pp.runnext.ptr())
		pp.runnext = 0
	}
	if len(pp.timers) > 0 {
		plocal := getg().m.p.ptr()
		// The world is stopped, but we acquire timersLock to
		// protect against sysmon calling timeSleepUntil.
		// This is the only case where we hold the timersLock of
		// more than one P, so there are no deadlock concerns.
		lock(&plocal.timersLock)
		lock(&pp.timersLock)
		moveTimers(plocal, pp.timers)
		pp.timers = nil
		pp.numTimers = 0
		pp.adjustTimers = 0
		pp.deletedTimers = 0
		atomic.Store64(&pp.timer0When, 0)
		unlock(&pp.timersLock)
		unlock(&plocal.timersLock)
	}
	// If there's a background worker, make it runnable and put
	// it on the global queue so it can clean itself up.
	if gp := pp.gcBgMarkWorker.ptr(); gp != nil {
		casgstatus(gp, _Gwaiting, _Grunnable)
		if trace.enabled {
			traceGoUnpark(gp, 0)
		}
		globrunqput(gp)
		// This assignment doesn't race because the
		// world is stopped.
		pp.gcBgMarkWorker.set(nil)
	}
	// Flush p's write barrier buffer.
	if gcphase != _GCoff {
		wbBufFlush1(pp)
		pp.gcw.dispose()
	}
	for i := range pp.sudogbuf {
		pp.sudogbuf[i] = nil
	}
	pp.sudogcache = pp.sudogbuf[:0]
	for i := range pp.deferpool {
		for j := range pp.deferpoolbuf[i] {
			pp.deferpoolbuf[i][j] = nil
		}
		pp.deferpool[i] = pp.deferpoolbuf[i][:0]
	}
	systemstack(func() {
		for i := 0; i < pp.mspancache.len; i++ {
			// Safe to call since the world is stopped.
			mheap_.spanalloc.free(unsafe.Pointer(pp.mspancache.buf[i]))
		}
		pp.mspancache.len = 0
		pp.pcache.flush(&mheap_.pages)
	})
	freemcache(pp.mcache)
	pp.mcache = nil
	gfpurge(pp)
	traceProcFree(pp)
	if raceenabled {
		if pp.timerRaceCtx != 0 {
			// The race detector code uses a callback to fetch
			// the proc context, so arrange for that callback
			// to see the right thing.
			// This hack only works because we are the only
			// thread running.
			mp := getg().m
			phold := mp.p.ptr()
			mp.p.set(pp)

			racectxend(pp.timerRaceCtx)
			pp.timerRaceCtx = 0

			mp.p.set(phold)
		}
		raceprocdestroy(pp.raceprocctx)
		pp.raceprocctx = 0
	}
	pp.gcAssistTime = 0
	pp.status = _Pdead
}

// Change number of processors. The world is stopped, sched is locked.
// gcworkbufs are not being modified by either the GC or
// the write barrier code.
// Returns list of Ps with local work, they need to be scheduled by the caller.
// 1. 如果全局变量 allp 切片中的处理器数量少于期望数量，就会对切片进行扩容；
// 2. 使用 new 创建新的处理器结构体并调用 runtime.p.init 方法初始化刚刚扩容的处理器；
// 3. 通过指针将线程 m0 和处理器 allp[0] 绑定到一起；
// 4. 调用 runtime.p.destroy 方法释放不再使用的处理器结构；
// 5. 通过截断改变全局变量 allp 的长度保证与期望处理器数量相等；
// 6. 将除 allp[0] 之外的处理器 P 全部设置成 _Pidle 并加入到全局的空闲队列中；
// 调用 runtime.procresize 就是调度器启动的最后一步，在这一步过后调度器会完成相应数量处理器的启动，
// 等待用户创建运行新的 Goroutine 并为 Goroutine 调度处理器资源。
func procresize(nprocs int32) *p {
	old := gomaxprocs
	if old < 0 || nprocs <= 0 {
		throw("procresize: invalid arg")
	}
	if trace.enabled {
		traceGomaxprocs(nprocs)
	}

	// update statistics
	now := nanotime()
	if sched.procresizetime != 0 {
		sched.totaltime += int64(old) * (now - sched.procresizetime)
	}
	sched.procresizetime = now

	// Grow allp if necessary.
	// 1. 如果全局变量 allp 切片中的处理器数量少于期望数量，就会对切片进行扩容；
	// nprocs 就是期望数量
	if nprocs > int32(len(allp)) {
		// Synchronize with retake, which could be running
		// concurrently since it doesn't run on a P.
		lock(&allpLock)
		if nprocs <= int32(cap(allp)) {
			allp = allp[:nprocs]
		} else {
			nallp := make([]*p, nprocs)
			// Copy everything up to allp's cap so we
			// never lose old allocated Ps.
			copy(nallp, allp[:cap(allp)])
			allp = nallp
		}
		unlock(&allpLock)
	}

	// 初始化新的P
	// initialize new P's

	// 2. 使用 new 创建新的处理器结构体并调用 runtime.p.init 方法初始化刚刚创建的处理器；
	for i := old; i < nprocs; i++ {
		pp := allp[i]
		if pp == nil {
			pp = new(p)
		}
		pp.init(i)
		atomicstorep(unsafe.Pointer(&allp[i]), unsafe.Pointer(pp))
	}

	_g_ := getg()
	if _g_.m.p != 0 && _g_.m.p.ptr().id < nprocs {
		// continue to use the current P
		_g_.m.p.ptr().status = _Prunning
		_g_.m.p.ptr().mcache.prepareForSweep()
	} else {
		// release the current P and acquire allp[0].
		//
		// We must do this before destroying our current P
		// because p.destroy itself has write barriers, so we
		// need to do that from a valid P.
		if _g_.m.p != 0 {
			if trace.enabled {
				// Pretend that we were descheduled
				// and then scheduled again to keep
				// the trace sane.
				traceGoSched()
				traceProcStop(_g_.m.p.ptr())
			}
			_g_.m.p.ptr().m = 0
		}
		_g_.m.p = 0
		// 3. 通过指针将线程 m0 和处理器 allp[0] 绑定到一起；
		p := allp[0]
		p.m = 0
		p.status = _Pidle
		acquirep(p)
		if trace.enabled {
			traceGoStart()
		}
	}

	// g.m.p is now set, so we no longer need mcache0 for bootstrapping.
	mcache0 = nil

	// release resources from unused P's
	// 4. 调用 runtime.p.destroy 方法释放不再使用的处理器结构；
	for i := nprocs; i < old; i++ {
		p := allp[i]
		p.destroy()
		// can't free P itself because it can be referenced by an M in syscall
	}

	// Trim allp.
	// 5. 通过截断改变全局变量 allp 的长度保证与期望处理器数量相等；
	if int32(len(allp)) != nprocs {
		lock(&allpLock)
		allp = allp[:nprocs]
		unlock(&allpLock)
	}

	// 6. 将除 allp[0] 之外的处理器 P 全部设置成 _Pidle 并加入到全局的空闲队列中；
	var runnablePs *p
	for i := nprocs - 1; i >= 0; i-- {
		p := allp[i]
		if _g_.m.p.ptr() == p {
			continue
		}
		// XGF: 把 运行队列为空的 P，放到空闲P列表
		p.status = _Pidle
		if runqempty(p) {
			// 放到空闲队列
			pidleput(p)
		} else {
			p.m.set(mget())
			p.link.set(runnablePs)
			runnablePs = p
		}
	}
	stealOrder.reset(uint32(nprocs))
	var int32p *int32 = &gomaxprocs // make compiler check that gomaxprocs is an int32
	atomic.Store((*uint32)(unsafe.Pointer(int32p)), uint32(nprocs))
	return runnablePs
}

// Associate p and the current m.
//
// This function is allowed to have write barriers even if the caller
// isn't because it immediately acquires _p_.
//
//go:yeswritebarrierrec
func acquirep(_p_ *p) {
	// Do the part that isn't allowed to have write barriers.
	wirep(_p_)

	// Have p; write barriers now allowed.

	// Perform deferred mcache flush before this P can allocate
	// from a potentially stale mcache.
	_p_.mcache.prepareForSweep()

	if trace.enabled {
		traceProcStart()
	}
}

// wirep is the first step of acquirep, which actually associates the
// current M to _p_. This is broken out so we can disallow write
// barriers for this part, since we don't yet have a P.
//
//go:nowritebarrierrec
//go:nosplit
func wirep(_p_ *p) {
	_g_ := getg()

	if _g_.m.p != 0 {
		throw("wirep: already in go")
	}
	if _p_.m != 0 || _p_.status != _Pidle {
		id := int64(0)
		if _p_.m != 0 {
			id = _p_.m.ptr().id
		}
		print("wirep: p->m=", _p_.m, "(", id, ") p->status=", _p_.status, "\n")
		throw("wirep: invalid p state")
	}
	_g_.m.p.set(_p_)
	_p_.m.set(_g_.m)
	_p_.status = _Prunning
}

// Disassociate p and the current m.
func releasep() *p {
	_g_ := getg()

	if _g_.m.p == 0 {
		throw("releasep: invalid arg")
	}
	_p_ := _g_.m.p.ptr()
	if _p_.m.ptr() != _g_.m || _p_.status != _Prunning {
		print("releasep: m=", _g_.m, " m->p=", _g_.m.p.ptr(), " p->m=", hex(_p_.m), " p->status=", _p_.status, "\n")
		throw("releasep: invalid p state")
	}
	if trace.enabled {
		traceProcStop(_g_.m.p.ptr())
	}
	_g_.m.p = 0
	_p_.m = 0
	_p_.status = _Pidle
	return _p_
}

func incidlelocked(v int32) {
	lock(&sched.lock)
	sched.nmidlelocked += v
	if v > 0 {
		checkdead()
	}
	unlock(&sched.lock)
}

// Check for deadlock situation.
// The check is based on number of running M's, if 0 -> deadlock.
// sched.lock must be held.
func checkdead() {
	// For -buildmode=c-shared or -buildmode=c-archive it's OK if
	// there are no running goroutines. The calling program is
	// assumed to be running.
	// 如果当前是以 c-shared 或 c-archive 模式构建的，
	// 那么即使没有正在运行的 Goroutine 也是可以的，因为调用程序被假定为正在运行。
	if islibrary || isarchive {
		return
	}

	// If we are dying because of a signal caught on an already idle thread,
	// freezetheworld will cause all running threads to block.
	// And runtime will essentially enter into deadlock state,
	// except that there is a thread that will call exit soon.
	// 如果由于信号导致程序正在崩溃（panicking > 0），则不进行死锁检查，因为程序即将退出。
	if panicking > 0 {
		return
	}

	// If we are not running under cgo, but we have an extra M then account
	// for it. (It is possible to have an extra M on Windows without cgo to
	// accommodate callbacks created by syscall.NewCallback. See issue #6751
	// for details.)
	var run0 int32
	if !iscgo && cgoHasExtraM {
		mp := lockextra(true)
		haveExtraM := extraMCount > 0
		unlockextra(mp)
		if haveExtraM {
			run0 = 1
		}
	}

	run := mcount() - sched.nmidle - sched.nmidlelocked - sched.nmsys
	if run > run0 {
		return
	}
	if run < 0 {
		print("runtime: checkdead: nmidle=", sched.nmidle, " nmidlelocked=", sched.nmidlelocked, " mcount=", mcount(), " nmsys=", sched.nmsys, "\n")
		throw("checkdead: inconsistent counts")
	}

	grunning := 0
	lock(&allglock)
	for i := 0; i < len(allgs); i++ {
		gp := allgs[i]
		if isSystemGoroutine(gp, false) {
			continue
		}
		s := readgstatus(gp)
		switch s &^ _Gscan {
		case _Gwaiting,
			_Gpreempted:
			grunning++
		case _Grunnable,
			_Grunning,
			_Gsyscall:
			unlock(&allglock)
			print("runtime: checkdead: find g ", gp.goid, " in status ", s, "\n")
			throw("checkdead: runnable g")
		}
	}
	unlock(&allglock)
	if grunning == 0 { // possible if main goroutine calls runtime·Goexit()
		unlock(&sched.lock) // unlock so that GODEBUG=scheddetail=1 doesn't hang
		throw("no goroutines (main called runtime.Goexit) - deadlock!")
	}

	// Maybe jump time forward for playground.
	if faketime != 0 {
		when, _p_ := timeSleepUntil()
		if _p_ != nil {
			faketime = when
			for pp := &sched.pidle; *pp != 0; pp = &(*pp).ptr().link {
				if (*pp).ptr() == _p_ {
					*pp = _p_.link
					break
				}
			}
			mp := mget()
			if mp == nil {
				// There should always be a free M since
				// nothing is running.
				throw("checkdead: no m for timer")
			}
			mp.nextp.set(_p_)
			notewakeup(&mp.park)
			return
		}
	}

	// There are no goroutines running, so we can look at the P's.
	for _, _p_ := range allp {
		if len(_p_.timers) > 0 {
			return
		}
	}

	getg().m.throwing = -1 // do not dump full stacks
	unlock(&sched.lock)    // unlock so that GODEBUG=scheddetail=1 doesn't hang
	throw("all goroutines are asleep - deadlock!")
}

// forcegcperiod is the maximum time in nanoseconds between garbage
// collections. If we go this long without a garbage collection, one
// is forced to run.
//
// This is a variable for testing purposes. It normally doesn't change.
var forcegcperiod int64 = 2 * 60 * 1e9

// Always runs without a P, so write barriers are not allowed.
// 第一个GC关键组件，系统监控：
//
//go:nowritebarrierrec
func sysmon() {
	lock(&sched.lock)
	sched.nmsys++
	checkdead()
	unlock(&sched.lock)

	lasttrace := int64(0)
	idle := 0 // how many cycles in succession we had not wokeup somebody
	delay := uint32(0)

	// 这个循环中，sched.gcwaiting 的初始值为 0，表示不需要进行垃圾回收；如果值为 1 则表明正在等待垃圾回收的完成，需要进入休眠状态。
	// 因此在用户态代码开始时，会直接进入下一个条件。第二个条件需要检查 forcegc 这个全局变量
	for {
		if idle == 0 { // start with 20us sleep...
			delay = 20
		} else if idle > 50 { // start doubling the sleep after 1ms...
			delay *= 2
		}
		if delay > 10*1000 { // up to 10ms
			delay = 10 * 1000
		}
		usleep(delay)
		now := nanotime()
		next, _ := timeSleepUntil()
		if debug.schedtrace <= 0 && (sched.gcwaiting != 0 || atomic.Load(&sched.npidle) == uint32(gomaxprocs)) {
			lock(&sched.lock)
			if atomic.Load(&sched.gcwaiting) != 0 || atomic.Load(&sched.npidle) == uint32(gomaxprocs) {
				if next > now {
					atomic.Store(&sched.sysmonwait, 1)
					unlock(&sched.lock)
					// Make wake-up period small enough
					// for the sampling to be correct.
					sleep := forcegcperiod / 2
					if next-now < sleep {
						sleep = next - now
					}
					shouldRelax := sleep >= osRelaxMinNS
					if shouldRelax {
						osRelax(true)
					}
					notetsleep(&sched.sysmonnote, sleep)
					if shouldRelax {
						osRelax(false)
					}
					now = nanotime()
					next, _ = timeSleepUntil()
					lock(&sched.lock)
					atomic.Store(&sched.sysmonwait, 0)
					noteclear(&sched.sysmonnote)
				}
				idle = 0
				delay = 20
			}
			unlock(&sched.lock)
		}
		lock(&sched.sysmonlock)
		{
			// If we spent a long time blocked on sysmonlock
			// then we want to update now and next since it's
			// likely stale.
			now1 := nanotime()
			if now1-now > 50*1000 /* 50µs */ {
				next, _ = timeSleepUntil()
			}
			now = now1
		}

		// trigger libc interceptors if needed
		if *cgo_yield != nil {
			asmcgocall(*cgo_yield, nil)
		}
		// poll network if not polled for more than 10ms
		lastpoll := int64(atomic.Load64(&sched.lastpoll))
		if netpollinited() && lastpoll != 0 && lastpoll+10*1000*1000 < now {
			atomic.Cas64(&sched.lastpoll, uint64(lastpoll), uint64(now))
			list := netpoll(0) // non-blocking - returns list of goroutines
			if !list.empty() {
				// Need to decrement number of idle locked M's
				// (pretending that one more is running) before injectglist.
				// Otherwise it can lead to the following situation:
				// injectglist grabs all P's but before it starts M's to run the P's,
				// another M returns from syscall, finishes running its G,
				// observes that there is no work to do and no other running M's
				// and reports deadlock.
				incidlelocked(-1)
				injectglist(&list)
				incidlelocked(1)
			}
		}
		if next < now {
			// There are timers that should have already run,
			// perhaps because there is an unpreemptible P.
			// Try to start an M to run them.
			startm(nil, false)
		}
		if atomic.Load(&scavenge.sysmonWake) != 0 {
			// Kick the scavenger awake if someone requested it.
			wakeScavenger()
		}
		// retake P's blocked in syscalls
		// and preempt long running G's
		// 在系统监控中，如果一个 Goroutine 的运行时间超过 10ms，就会调用 runtime.retake，runtime.retake 会调用 runtime.preemptone；
		if retake(now) != 0 {
			idle = 0
		} else {
			idle++
		}
		// check if we need to force a GC
		// 检查是否需要强制GC
		// forcegc 这个全局变量的初始值为 0，这时条件 atomic.Load(&forcegc.idle) != 0 为 false
		if t := (gcTrigger{kind: gcTriggerTime, now: now}); t.test() && atomic.Load(&forcegc.idle) != 0 {
			// 系统监控在每个循环中都会主动构建一个 runtime.gcTrigger 并检查垃圾收集的触发条件是否满足，
			// 如果满足条件，系统监控会将 runtime.forcegc 状态中持有的 Goroutine 加入全局队列等待调度器的调度。
			lock(&forcegc.lock)
			forcegc.idle = 0
			var list gList
			list.push(forcegc.g)
			// injectlist 会将 forcegc.g 强制加入调度器调度队列中，等待执行 GC 调度
			injectglist(&list)
			unlock(&forcegc.lock)
		}
		if debug.schedtrace > 0 && lasttrace+int64(debug.schedtrace)*1000000 <= now {
			lasttrace = now
			schedtrace(debug.scheddetail > 0)
		}
		unlock(&sched.sysmonlock)
	}
}

type sysmontick struct {
	schedtick   uint32
	schedwhen   int64
	syscalltick uint32
	syscallwhen int64
}

// forcePreemptNS is the time slice given to a G before it is
// preempted.
const forcePreemptNS = 10 * 1000 * 1000 // 10ms

func retake(now int64) uint32 {
	n := 0
	// Prevent allp slice changes. This lock will be completely
	// uncontended unless we're already stopping the world.
	lock(&allpLock)
	// We can't use a range loop over allp because we may
	// temporarily drop the allpLock. Hence, we need to re-fetch
	// allp each time around the loop.
	for i := 0; i < len(allp); i++ {
		_p_ := allp[i]
		if _p_ == nil {
			// This can happen if procresize has grown
			// allp but not yet created new Ps.
			continue
		}
		pd := &_p_.sysmontick
		s := _p_.status
		sysretake := false
		if s == _Prunning || s == _Psyscall {
			// Preempt G if it's running for too long.
			t := int64(_p_.schedtick)
			if int64(pd.schedtick) != t {
				pd.schedtick = uint32(t)
				pd.schedwhen = now
			} else if pd.schedwhen+forcePreemptNS <= now {
				// 如果当前G的运行时间大于 10 ms，则会被标志位可抢占。
				preemptone(_p_)
				// In case of syscall, preemptone() doesn't
				// work, because there is no M wired to P.
				sysretake = true
			}
		}
		if s == _Psyscall {
			// Retake P from syscall if it's there for more than 1 sysmon tick (at least 20us).
			t := int64(_p_.syscalltick)
			if !sysretake && int64(pd.syscalltick) != t {
				pd.syscalltick = uint32(t)
				pd.syscallwhen = now
				continue
			}
			// On the one hand we don't want to retake Ps if there is no other work to do,
			// but on the other hand we want to retake them eventually
			// because they can prevent the sysmon thread from deep sleep.
			if runqempty(_p_) && atomic.Load(&sched.nmspinning)+atomic.Load(&sched.npidle) > 0 && pd.syscallwhen+10*1000*1000 > now {
				continue
			}
			// Drop allpLock so we can take sched.lock.
			unlock(&allpLock)
			// Need to decrement number of idle locked M's
			// (pretending that one more is running) before the CAS.
			// Otherwise the M from which we retake can exit the syscall,
			// increment nmidle and report deadlock.
			incidlelocked(-1)
			if atomic.Cas(&_p_.status, s, _Pidle) {
				if trace.enabled {
					traceGoSysBlock(_p_)
					traceProcStop(_p_)
				}
				n++
				_p_.syscalltick++
				handoffp(_p_)
			}
			incidlelocked(1)
			lock(&allpLock)
		}
	}
	unlock(&allpLock)
	return uint32(n)
}

// Tell all goroutines that they have been preempted and they should stop.
// This function is purely best-effort. It can fail to inform a goroutine if a
// processor just started running it.
// No locks need to be held.
// Returns true if preemption request was issued to at least one goroutine.

// 在 runtime.stoptheworld 中调用 runtime.preemptall 设置所有处理器上正在运行的 Goroutine 的 stackguard0 为 StackPreempt；
// 在 runtime.newstack 函数中增加抢占的代码，当 stackguard0 等于 StackPreempt 时触发调度器抢占让出线程；
func preemptall() bool {
	res := false
	for _, _p_ := range allp {
		if _p_.status != _Prunning {
			continue
		}
		if preemptone(_p_) {
			res = true
		}
	}
	return res
}

// Tell the goroutine running on processor P to stop.
// This function is purely best-effort. It can incorrectly fail to inform the goroutine.
// It can send inform the wrong goroutine.
// Even if it informs the correct goroutine, that goroutine might ignore the request if it is simultaneously executing newstack.
// No lock needs to be held.
// Returns true if preemption request was issued.
// The actual preemption will happen at some point in the future and will be indicated by the gp->status no longer being Grunning
// 告诉处理器P上运行的goroutine停止。
// 此功能纯粹是尽力而为。它可能会错误地无法通知goroutine。
// 它可以发送通知给错误的goroutine。
// 即使它通知了正确的goroutine，该goroutine如果同时执行newstack也可能会忽略该请求。
// 无需锁定。
// 如果发出了抢占请求，则返回true。
// 实际的抢占将在将来的某个时刻发生，并将通过gp-> status不再显示
func preemptone(_p_ *p) bool {
	mp := _p_.m.ptr()
	if mp == nil || mp == getg().m {
		return false
	}
	gp := mp.curg
	if gp == nil || gp == mp.g0 {
		return false
	}

	gp.preempt = true

	// Every call in a go routine checks for stack overflow by comparing the current stack pointer to gp->stackguard0.
	// go例程中的每个调用都通过将当前堆栈指针与gp-> stackguard0比较来检查堆栈溢出。
	// Setting gp->stackguard0 to StackPreempt folds preemption into the normal stack overflow check.
	// 将gp-> stackguard0设置为StackPreempt会将抢占折叠到正常的堆栈溢出检查中。

	// XGF: 将当前G的stackguard0 设置为 stackPreempt，因为者当前G可以被抢占。
	gp.stackguard0 = stackPreempt

	// Request an async preemption of this P.
	if preemptMSupported && debug.asyncpreemptoff == 0 {
		// 设置preempt表示该P应该尽快进入调度程序（无论G正在运行什么）
		_p_.preempt = true
		// 调用 runtime.preemptM 触发抢占
		preemptM(mp)
	}

	return true
}

var starttime int64

func schedtrace(detailed bool) {
	now := nanotime()
	if starttime == 0 {
		starttime = now
	}

	lock(&sched.lock)
	print("SCHED ", (now-starttime)/1e6, "ms: gomaxprocs=", gomaxprocs, " idleprocs=", sched.npidle, " threads=", mcount(), " spinningthreads=", sched.nmspinning, " idlethreads=", sched.nmidle, " runqueue=", sched.runqsize)
	if detailed {
		print(" gcwaiting=", sched.gcwaiting, " nmidlelocked=", sched.nmidlelocked, " stopwait=", sched.stopwait, " sysmonwait=", sched.sysmonwait, "\n")
	}
	// We must be careful while reading data from P's, M's and G's.
	// Even if we hold schedlock, most data can be changed concurrently.
	// E.g. (p->m ? p->m->id : -1) can crash if p->m changes from non-nil to nil.
	for i, _p_ := range allp {
		mp := _p_.m.ptr()
		h := atomic.Load(&_p_.runqhead)
		t := atomic.Load(&_p_.runqtail)
		if detailed {
			id := int64(-1)
			if mp != nil {
				id = mp.id
			}
			print("  P", i, ": status=", _p_.status, " schedtick=", _p_.schedtick, " syscalltick=", _p_.syscalltick, " m=", id, " runqsize=", t-h, " gfreecnt=", _p_.gFree.n, " timerslen=", len(_p_.timers), "\n")
		} else {
			// In non-detailed mode format lengths of per-P run queues as:
			// [len1 len2 len3 len4]
			print(" ")
			if i == 0 {
				print("[")
			}
			print(t - h)
			if i == len(allp)-1 {
				print("]\n")
			}
		}
	}

	if !detailed {
		unlock(&sched.lock)
		return
	}

	for mp := allm; mp != nil; mp = mp.alllink {
		_p_ := mp.p.ptr()
		gp := mp.curg
		lockedg := mp.lockedg.ptr()
		id1 := int32(-1)
		if _p_ != nil {
			id1 = _p_.id
		}
		id2 := int64(-1)
		if gp != nil {
			id2 = gp.goid
		}
		id3 := int64(-1)
		if lockedg != nil {
			id3 = lockedg.goid
		}
		print("  M", mp.id, ": p=", id1, " curg=", id2, " mallocing=", mp.mallocing, " throwing=", mp.throwing, " preemptoff=", mp.preemptoff, ""+" locks=", mp.locks, " dying=", mp.dying, " spinning=", mp.spinning, " blocked=", mp.blocked, " lockedg=", id3, "\n")
	}

	lock(&allglock)
	for gi := 0; gi < len(allgs); gi++ {
		gp := allgs[gi]
		mp := gp.m
		lockedm := gp.lockedm.ptr()
		id1 := int64(-1)
		if mp != nil {
			id1 = mp.id
		}
		id2 := int64(-1)
		if lockedm != nil {
			id2 = lockedm.id
		}
		print("  G", gp.goid, ": status=", readgstatus(gp), "(", gp.waitreason.String(), ") m=", id1, " lockedm=", id2, "\n")
	}
	unlock(&allglock)
	unlock(&sched.lock)
}

// schedEnableUser enables or disables the scheduling of user
// goroutines.
//
// This does not stop already running user goroutines, so the caller
// should first stop the world when disabling user goroutines.
func schedEnableUser(enable bool) {
	lock(&sched.lock)
	if sched.disable.user == !enable {
		unlock(&sched.lock)
		return
	}
	sched.disable.user = !enable
	if enable {
		n := sched.disable.n
		sched.disable.n = 0
		globrunqputbatch(&sched.disable.runnable, n)
		unlock(&sched.lock)
		for ; n != 0 && sched.npidle != 0; n-- {
			startm(nil, false)
		}
	} else {
		unlock(&sched.lock)
	}
}

// schedEnabled reports whether gp should be scheduled. It returns
// false is scheduling of gp is disabled.
func schedEnabled(gp *g) bool {
	if sched.disable.user {
		return isSystemGoroutine(gp, true)
	}
	return true
}

// Put mp on midle list.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func mput(mp *m) {
	mp.schedlink = sched.midle
	sched.midle.set(mp)
	sched.nmidle++
	checkdead()
}

/*
	用于从 midle 列表中获取一个空闲的 m（工作线程）。
	1.尝试从 midle 列表中获取一个 m。
	2.调度器必须被锁定，以确保线程安全。
	3.该函数可能在 STW（Stop The World）期间运行，因此不允许使用写屏障。
*/
// Try to get an m from midle list.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func mget() *m {
	// 从调度器的空闲工作线程列表 sched.midle 中获取一个工作线程。
	mp := sched.midle.ptr()
	if mp != nil {
		// 将空闲工作线程列表的头部更新为下一个空闲工作线程。
		sched.midle = mp.schedlink
		// 将空闲工作线程计数 sched.nmidle 减 1。
		sched.nmidle--
	}
	return mp
}

// Put gp on the global runnable queue.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func globrunqput(gp *g) {
	sched.runq.pushBack(gp)
	sched.runqsize++
}

// Put gp at the head of the global runnable queue.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func globrunqputhead(gp *g) {
	sched.runq.push(gp)
	sched.runqsize++
}

// Put a batch of runnable goroutines on the global runnable queue.
// This clears *batch.
// Sched must be locked.
func globrunqputbatch(batch *gQueue, n int32) {
	sched.runq.pushBackAll(*batch)
	sched.runqsize += n
	*batch = gQueue{}
}

/*
	全局队列这么设计的目的：
	1.负载均衡：通过从全局队列中获取一批 Goroutine 并将它们分配到本地队列，globrunqget 函数帮助实现 Goroutine 的负载均衡。
    2.高效调度：通过批量获取 Goroutine，减少了对全局队列的访问频率，从而提高了调度的效率。
	3.并发安全：函数注释中提到 "Sched must be locked"，意味着在调用此函数时，调度器必须被锁定，以确保对全局队列的访问是线程安全的。
*/
// Try get a batch of G's from the global runnable queue.
// Sched must be locked.
func globrunqget(_p_ *p, max int32) *g {
	// 如果全局可运行队列的大小为零，直接返回 nil，表示没有可获取的 Goroutine。
	if sched.runqsize == 0 {
		return nil
	}

	// 初始值为 sched.runqsize/gomaxprocs + 1，即全局队列大小除以最大处理器数加一。
	n := sched.runqsize/gomaxprocs + 1
	// 如果 n 大于全局队列大小，则将 n 设置为全局队列大小。
	if n > sched.runqsize {
		n = sched.runqsize
	}
	// 如果 max 大于零且 n 大于 max，则将 n 设置为 max。
	if max > 0 && n > max {
		n = max
	}
	// 如果 n 大于本地队列长度的一半，则将 n 设置为本地队列长度的一半。
	if n > int32(len(_p_.runq))/2 {
		n = int32(len(_p_.runq)) / 2
	}

	// 从全局队列大小中减去 n，更新全局队列的大小。
	sched.runqsize -= n

	// 从全局队列中弹出一个 Goroutine，并将其赋值给 gp。
	gp := sched.runq.pop()
	n--
	for ; n > 0; n-- {
		// 继续从全局队列中弹出 n-1 个 Goroutine，并将它们放入本地队列中。
		gp1 := sched.runq.pop()
		runqput(_p_, gp1, false)
	}
	// 返回第一个获取的 Goroutine gp。
	return gp
}

// Put p to on _Pidle list.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func pidleput(_p_ *p) {
	if !runqempty(_p_) {
		throw("pidleput: P has non-empty run queue")
	}
	_p_.link = sched.pidle
	sched.pidle.set(_p_)
	atomic.Xadd(&sched.npidle, 1) // TODO: fast atomic
}

/*
	用于从 _Pidle 列表中获取一个空闲的 p（处理器）。
	1. 尝试从 _Pidle 列表中获取一个 p。
	2. 调度器必须被锁定，以确保线程安全。
	3. 该函数可能在 STW（Stop The World）期间运行，因此不允许使用写屏障。
*/
// Try get a p from _Pidle list.
// Sched must be locked.
// May run during STW, so write barriers are not allowed.
//go:nowritebarrierrec
func pidleget() *p {
	// 从调度器的空闲处理器列表 sched.pidle 中获取一个处理器。
	_p_ := sched.pidle.ptr()
	if _p_ != nil {
		// 将空闲处理器列表的头部更新为下一个空闲处理器。
		// _p_.link 指向下一个空闲处理器。
		sched.pidle = _p_.link
		// 使用原子操作将空闲处理器计数 sched.npidle 减 1。
		atomic.Xadd(&sched.npidle, -1) // TODO: fast atomic
	}
	return _p_
}

// runqempty reports whether _p_ has no Gs on its local run queue.
// It never returns true spuriously.
func runqempty(_p_ *p) bool {
	// Defend against a race where 1) _p_ has G1 in runqnext but runqhead == runqtail,
	// 2) runqput on _p_ kicks G1 to the runq, 3) runqget on _p_ empties runqnext.
	// Simply observing that runqhead == runqtail and then observing that runqnext == nil
	// does not mean the queue is empty.
	for {
		head := atomic.Load(&_p_.runqhead)
		tail := atomic.Load(&_p_.runqtail)
		runnext := atomic.Loaduintptr((*uintptr)(unsafe.Pointer(&_p_.runnext)))
		if tail == atomic.Load(&_p_.runqtail) {
			return head == tail && runnext == 0
		}
	}
}

// To shake out latent assumptions about scheduling order,
// we introduce some randomness into scheduling decisions
// when running with the race detector.
// The need for this was made obvious by changing the
// (deterministic) scheduling order in Go 1.5 and breaking
// many poorly-written tests.
// With the randomness here, as long as the tests pass
// consistently with -race, they shouldn't have latent scheduling
// assumptions.
const randomizeScheduler = raceenabled

/*
	用于将一个 goroutine g 放入本地可运行队列中。
	它根据 next 参数决定是将 g 放在队列的尾部还是放在 _p_ 的 runnext 槽中。
	如果本地队列已满，则将 g 放入全局队列中。这个函数只能由拥有该 P（处理器）的线程执行。
*/
// runqput tries to put g on the local runnable queue.
// If next is false, runqput adds g to the tail of the runnable queue.
// If next is true, runqput puts g in the _p_.runnext slot.
// If the run queue is full, runnext puts g on the global queue.
// Executed only by the owner P.
func runqput(_p_ *p, gp *g, next bool) {
	// randomizeScheduler 是一个布尔值，表示是否启用随机化调度。
	// fastrand()%2 == 0：使用快速随机数生成器生成一个随机数，并检查其是否为偶数。
	// 如果启用了随机化调度且 next 为 true，并且随机数为偶数，则将 next 设置为 false。
	// 也就是说：如果启用了随机化调度，则有一定概率【50%】将 next 设置为 false，以避免总是将 goroutine 放在 runnext 槽中。
	if randomizeScheduler && next && fastrand()%2 == 0 {
		next = false
	}

	if next {
	retryNext:
		// oldnext := _p_.runnext：读取 _p_ 的 runnext 槽的当前值。
		oldnext := _p_.runnext
		// cas 是一个原子操作，用于将 _p_.runnext 的值从 oldnext 更改为 gp。
		if !_p_.runnext.cas(oldnext, guintptr(unsafe.Pointer(gp))) {
			// 如果 CAS 操作失败，则跳转到 retryNext 标签，重试操作。
			goto retryNext
		}
		// if oldnext == 0：如果 oldnext 为 0，表示 runnext 槽之前是空的。
		if oldnext == 0 {
			// 直接返回，因为 gp 已成功放入 runnext 槽中。
			return
		}
		// Kick the old runnext out to the regular run queue.
		// 如果 oldnext 不为 0，则将 oldnext 转换为 g 指针，并将其赋值给 gp。
		// 这行代码的作用是将之前在 runnext 槽中的 goroutine oldnext 放入常规的运行队列中。
		gp = oldnext.ptr()
	}

	// retry:：标签，用于在操作失败时重试。
retry:
	/* 这段代码的功能是：将 goroutine gp 放入本地运行队列的逻辑。*/
	h := atomic.LoadAcq(&_p_.runqhead) // load-acquire, synchronize with consumers
	t := _p_.runqtail
	// 队列未满，则执行一下操作： 将 gp 放入队列的尾部位置。
	if t-h < uint32(len(_p_.runq)) {
		// t%uint32(len(_p_.runq)) 计算队列的实际索引。
		// set 方法用于设置队列中的元素。
		_p_.runq[t%uint32(len(_p_.runq))].set(gp)
		// 更新队列尾部索引:
		// 使用原子操作更新 _p_.runqtail 的值。
		// StoreRel（存储-释放）操作确保新元素对消费者可见。
		atomic.StoreRel(&_p_.runqtail, t+1) // store-release, makes the item available for consumption
		return
	}
	// 处理本地运行队列已满的情况:
	// runqputslow 是一个函数，用于将 gp 放入全局运行队列中。
	if runqputslow(_p_, gp, h, t) {
		return
	}
	// 重试上述操作
	// the queue is not full, now the put above must succeed
	goto retry
}

// Put g and a batch of work from local runnable queue on global queue.
// Executed only by the owner P.
func runqputslow(_p_ *p, gp *g, h, t uint32) bool {
	var batch [len(_p_.runq)/2 + 1]*g

	// First, grab a batch from local queue.
	n := t - h
	n = n / 2
	if n != uint32(len(_p_.runq)/2) {
		throw("runqputslow: queue is not full")
	}
	for i := uint32(0); i < n; i++ {
		batch[i] = _p_.runq[(h+i)%uint32(len(_p_.runq))].ptr()
	}
	if !atomic.CasRel(&_p_.runqhead, h, h+n) { // cas-release, commits consume
		return false
	}
	batch[n] = gp

	if randomizeScheduler {
		for i := uint32(1); i <= n; i++ {
			j := fastrandn(i + 1)
			batch[i], batch[j] = batch[j], batch[i]
		}
	}

	// Link the goroutines.
	for i := uint32(0); i < n; i++ {
		batch[i].schedlink.set(batch[i+1])
	}
	var q gQueue
	q.head.set(batch[0])
	q.tail.set(batch[n])

	// Now put the batch on global queue.
	lock(&sched.lock)
	globrunqputbatch(&q, int32(n+1))
	unlock(&sched.lock)
	return true
}

// runqputbatch tries to put all the G's on q on the local runnable queue.
// If the queue is full, they are put on the global queue; in that case
// this will temporarily acquire the scheduler lock.
// Executed only by the owner P.
func runqputbatch(pp *p, q *gQueue, qsize int) {
	h := atomic.LoadAcq(&pp.runqhead)
	t := pp.runqtail
	n := uint32(0)
	for !q.empty() && t-h < uint32(len(pp.runq)) {
		gp := q.pop()
		pp.runq[t%uint32(len(pp.runq))].set(gp)
		t++
		n++
	}
	qsize -= int(n)

	if randomizeScheduler {
		off := func(o uint32) uint32 {
			return (pp.runqtail + o) % uint32(len(pp.runq))
		}
		for i := uint32(1); i < n; i++ {
			j := fastrandn(i + 1)
			pp.runq[off(i)], pp.runq[off(j)] = pp.runq[off(j)], pp.runq[off(i)]
		}
	}

	atomic.StoreRel(&pp.runqtail, t)
	if !q.empty() {
		globrunqputbatch(q, int32(qsize))
	}
}

// Get g from local runnable queue.
// If inheritTime is true, gp should inherit the remaining time in the
// current time slice. Otherwise, it should start a new time slice.
// Executed only by the owner P.
// runqget 函数的目的是从本地可运行队列中获取一个 goroutine。
// 如果 inheritTime 为 true，表示获取的 goroutine 应该继承当前时间片的剩余时间。
// 否则，应该开始一个新的时间片。
// 该函数仅由 P 的所有者执行。
func runqget(_p_ *p) (gp *g, inheritTime bool) {
	// If there's a runnext, it's the next G to run.
	// 获取 runnext，这是一个优化路径，用于快速获取下一个要运行的 goroutine。
	for {
		// 获取不到待运行的G，直接退出
		next := _p_.runnext
		if next == 0 {
			break
		}
		// 获取到待运行的G
		// 使用原子操作将 runnext 设置为 0，确保只有一个线程可以成功获取 runnext。
		// 即使能成功将 runnext 置为 0，则表示成功获取到 runnext。
		if _p_.runnext.cas(next, 0) {
			return next.ptr(), true
		}
	}

	for {
		// 使用原子加载获取 runqhead，这是一个加载-获取操作，用于与其他消费者同步。
		h := atomic.LoadAcq(&_p_.runqhead) // load-acquire, synchronize with other consumers
		t := _p_.runqtail
		// 如果队列为空（runqhead 等于 runqtail），返回 nil 和 false。
		if t == h {
			return nil, false
		}
		// 从队列中获取 goroutine。
		gp := _p_.runq[h%uint32(len(_p_.runq))].ptr()
		// 使用原子比较并交换操作更新 runqhead，这是一个比较并交换-释放操作，提交消费。
		if atomic.CasRel(&_p_.runqhead, h, h+1) { // cas-release, commits consume
			return gp, false
		}
	}
}

// Grabs a batch of goroutines from _p_'s runnable queue into batch.
// Batch is a ring buffer starting at batchHead.
// Returns number of grabbed goroutines.
// Can be executed by any P.
func runqgrab(_p_ *p, batch *[256]guintptr, batchHead uint32, stealRunNextG bool) uint32 {
	for {
		h := atomic.LoadAcq(&_p_.runqhead) // load-acquire, synchronize with other consumers
		t := atomic.LoadAcq(&_p_.runqtail) // load-acquire, synchronize with the producer
		n := t - h
		n = n - n/2
		if n == 0 {
			if stealRunNextG {
				// Try to steal from _p_.runnext.
				if next := _p_.runnext; next != 0 {
					if _p_.status == _Prunning {
						// Sleep to ensure that _p_ isn't about to run the g
						// we are about to steal.
						// The important use case here is when the g running
						// on _p_ ready()s another g and then almost
						// immediately blocks. Instead of stealing runnext
						// in this window, back off to give _p_ a chance to
						// schedule runnext. This will avoid thrashing gs
						// between different Ps.
						// A sync chan send/recv takes ~50ns as of time of
						// writing, so 3us gives ~50x overshoot.
						if GOOS != "windows" {
							usleep(3)
						} else {
							// On windows system timer granularity is
							// 1-15ms, which is way too much for this
							// optimization. So just yield.
							osyield()
						}
					}
					if !_p_.runnext.cas(next, 0) {
						continue
					}
					batch[batchHead%uint32(len(batch))] = next
					return 1
				}
			}
			return 0
		}
		if n > uint32(len(_p_.runq)/2) { // read inconsistent h and t
			continue
		}
		for i := uint32(0); i < n; i++ {
			g := _p_.runq[(h+i)%uint32(len(_p_.runq))]
			batch[(batchHead+i)%uint32(len(batch))] = g
		}
		if atomic.CasRel(&_p_.runqhead, h, h+n) { // cas-release, commits consume
			return n
		}
	}
}

// Steal half of elements from local runnable queue of p2
// and put onto local runnable queue of p.
// Returns one of the stolen elements (or nil if failed).
func runqsteal(_p_, p2 *p, stealRunNextG bool) *g {
	t := _p_.runqtail
	n := runqgrab(p2, &_p_.runq, t, stealRunNextG)
	if n == 0 {
		return nil
	}
	n--
	gp := _p_.runq[(t+n)%uint32(len(_p_.runq))].ptr()
	if n == 0 {
		return gp
	}
	h := atomic.LoadAcq(&_p_.runqhead) // load-acquire, synchronize with consumers
	if t-h+n >= uint32(len(_p_.runq)) {
		throw("runqsteal: runq overflow")
	}
	atomic.StoreRel(&_p_.runqtail, t+n) // store-release, makes the item available for consumption
	return gp
}

// A gQueue is a dequeue of Gs linked through g.schedlink. A G can only
// be on one gQueue or gList at a time.
type gQueue struct {
	head guintptr
	tail guintptr
}

// empty reports whether q is empty.
func (q *gQueue) empty() bool {
	return q.head == 0
}

// push adds gp to the head of q.
func (q *gQueue) push(gp *g) {
	gp.schedlink = q.head
	q.head.set(gp)
	if q.tail == 0 {
		q.tail.set(gp)
	}
}

// pushBack adds gp to the tail of q.
func (q *gQueue) pushBack(gp *g) {
	gp.schedlink = 0
	if q.tail != 0 {
		q.tail.ptr().schedlink.set(gp)
	} else {
		q.head.set(gp)
	}
	q.tail.set(gp)
}

// pushBackAll adds all Gs in l2 to the tail of q. After this q2 must
// not be used.
func (q *gQueue) pushBackAll(q2 gQueue) {
	if q2.tail == 0 {
		return
	}
	q2.tail.ptr().schedlink = 0
	if q.tail != 0 {
		q.tail.ptr().schedlink = q2.head
	} else {
		q.head = q2.head
	}
	q.tail = q2.tail
}

// pop removes and returns the head of queue q. It returns nil if
// q is empty.
func (q *gQueue) pop() *g {
	gp := q.head.ptr()
	if gp != nil {
		q.head = gp.schedlink
		if q.head == 0 {
			q.tail = 0
		}
	}
	return gp
}

// popList takes all Gs in q and returns them as a gList.
func (q *gQueue) popList() gList {
	stack := gList{q.head}
	*q = gQueue{}
	return stack
}

// A gList is a list of Gs linked through g.schedlink. A G can only be
// on one gQueue or gList at a time.
type gList struct {
	head guintptr
}

// empty reports whether l is empty.
func (l *gList) empty() bool {
	return l.head == 0
}

// push adds gp to the head of l.
func (l *gList) push(gp *g) {
	gp.schedlink = l.head
	l.head.set(gp)
}

// pushAll prepends all Gs in q to l.
func (l *gList) pushAll(q gQueue) {
	if !q.empty() {
		q.tail.ptr().schedlink = l.head
		l.head = q.head
	}
}

// pop removes and returns the head of l. If l is empty, it returns nil.
func (l *gList) pop() *g {
	gp := l.head.ptr()
	if gp != nil {
		l.head = gp.schedlink
	}
	return gp
}

//go:linkname setMaxThreads runtime/debug.setMaxThreads
func setMaxThreads(in int) (out int) {
	lock(&sched.lock)
	out = int(sched.maxmcount)
	if in > 0x7fffffff { // MaxInt32
		sched.maxmcount = 0x7fffffff
	} else {
		sched.maxmcount = int32(in)
	}
	checkmcount()
	unlock(&sched.lock)
	return
}

func haveexperiment(name string) bool {
	if name == "framepointer" {
		return framepointer_enabled // set by linker
	}
	x := sys.Goexperiment
	for x != "" {
		xname := ""
		i := index(x, ",")
		if i < 0 {
			xname, x = x, ""
		} else {
			xname, x = x[:i], x[i+1:]
		}
		if xname == name {
			return true
		}
		if len(xname) > 2 && xname[:2] == "no" && xname[2:] == name {
			return false
		}
	}
	return false
}

//go:nosplit
func procPin() int {
	_g_ := getg()
	mp := _g_.m

	mp.locks++
	return int(mp.p.ptr().id)
}

//go:nosplit
func procUnpin() {
	_g_ := getg()
	_g_.m.locks--
}

//go:linkname sync_runtime_procPin sync.runtime_procPin
//go:nosplit
func sync_runtime_procPin() int {
	return procPin()
}

//go:linkname sync_runtime_procUnpin sync.runtime_procUnpin
//go:nosplit
func sync_runtime_procUnpin() {
	procUnpin()
}

//go:linkname sync_atomic_runtime_procPin sync/atomic.runtime_procPin
//go:nosplit
func sync_atomic_runtime_procPin() int {
	return procPin()
}

//go:linkname sync_atomic_runtime_procUnpin sync/atomic.runtime_procUnpin
//go:nosplit
func sync_atomic_runtime_procUnpin() {
	procUnpin()
}

// Active spinning for sync.Mutex.
//go:linkname sync_runtime_canSpin sync.runtime_canSpin
//go:nosplit
func sync_runtime_canSpin(i int) bool {
	// sync.Mutex is cooperative, so we are conservative with spinning.
	// Spin only few times and only if running on a multicore machine and
	// GOMAXPROCS>1 and there is at least one other running P and local runq is empty.
	// As opposed to runtime mutex we don't do passive spinning here,
	// because there can be work on global runq or on other Ps.
	if i >= active_spin || ncpu <= 1 || gomaxprocs <= int32(sched.npidle+sched.nmspinning)+1 {
		return false
	}
	if p := getg().m.p.ptr(); !runqempty(p) {
		return false
	}
	return true
}

//go:linkname sync_runtime_doSpin sync.runtime_doSpin
//go:nosplit
func sync_runtime_doSpin() {
	procyield(active_spin_cnt)
}

var stealOrder randomOrder

// randomOrder 和 randomEnum 类型，它们是用于随机化工作窃取的辅助类型。
// 通过这些类型，可以在不重复的情况下以不同的伪随机顺序枚举所有的处理器（P）。
// 这种随机化有助于在多处理器环境中更均匀地分配工作负载，避免某些处理器过载而其他处理器空闲。
// 该算法基于以下事实：如果我们有一个数 X，使得 X 和 GOMAXPROCS 互质，那么序列 (i + X) % GOMAXPROCS 可以给出所需的枚举顺序。
// randomOrder/randomEnum are helper types for randomized work stealing.
// They allow to enumerate all Ps in different pseudo-random orders without repetitions.
// The algorithm is based on the fact that if we have X such that X and GOMAXPROCS
// are coprime, then a sequences of (i + X) % GOMAXPROCS gives the required enumeration.
type randomOrder struct {
	// 处理器的数量（通常是 GOMAXPROCS）。
	count uint32
	// 与 count 互质的数的列表。
	coprimes []uint32
}

type randomEnum struct {
	i     uint32
	count uint32
	pos   uint32
	inc   uint32
}

func (ord *randomOrder) reset(count uint32) {
	ord.count = count
	ord.coprimes = ord.coprimes[:0]
	for i := uint32(1); i <= count; i++ {
		if gcd(i, count) == 1 {
			ord.coprimes = append(ord.coprimes, i)
		}
	}
}

func (ord *randomOrder) start(i uint32) randomEnum {
	return randomEnum{
		count: ord.count,
		pos:   i % ord.count,
		inc:   ord.coprimes[i%uint32(len(ord.coprimes))],
	}
}

func (enum *randomEnum) done() bool {
	return enum.i == enum.count
}

func (enum *randomEnum) next() {
	enum.i++
	enum.pos = (enum.pos + enum.inc) % enum.count
}

func (enum *randomEnum) position() uint32 {
	return enum.pos
}

func gcd(a, b uint32) uint32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// An initTask represents the set of initializations that need to be done for a package.
// Keep in sync with ../../test/initempty.go:initTask
type initTask struct {
	// TODO: pack the first 3 fields more tightly?
	state uintptr // 0 = uninitialized, 1 = in progress, 2 = done
	ndeps uintptr
	nfns  uintptr
	// followed by ndeps instances of an *initTask, one per package depended on
	// followed by nfns pcs, one per init function to run
}

func doInit(t *initTask) {
	switch t.state {
	case 2: // fully initialized
		return
	case 1: // initialization in progress
		throw("recursive call during initialization - linker skew")
	default: // not initialized yet
		t.state = 1 // initialization in progress
		for i := uintptr(0); i < t.ndeps; i++ {
			p := add(unsafe.Pointer(t), (3+i)*sys.PtrSize)
			t2 := *(**initTask)(p)
			doInit(t2)
		}
		for i := uintptr(0); i < t.nfns; i++ {
			p := add(unsafe.Pointer(t), (3+t.ndeps+i)*sys.PtrSize)
			f := *(*func())(unsafe.Pointer(&p))
			f()
		}
		t.state = 2 // initialization done
	}
}
