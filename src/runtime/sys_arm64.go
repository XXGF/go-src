// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

// adjust Gobuf as if it executed a call to fn with context ctxt
// and then did an immediate Gosave.
func gostartcall(buf *gobuf, fn, ctxt unsafe.Pointer) {
	if buf.lr != 0 {
		throw("invalid use of gostartcall")
	}
	buf.lr = buf.pc
	buf.pc = uintptr(fn)
	buf.ctxt = ctxt
}

/*
// 新版：gostartcall

1. 调整 newg 的栈空间，把 goexit 函数的第二条指令的地址入栈，伪造成 goexit 函数调用了 fn 的假象，
	从而使 fn 执行完成后，执行 ret 指令时，返回到 goexit+1 处继续执行，完成最后的清理工作（不明白函数栈的同学，去看这个文章 13. 入门 go 语言汇编，看懂 GMP 源码 ）；
2. 重新设置 newg.buf.sp 指向新栈顶，设置 newg.buf.pc 为需要执行的函数的地址，即 fn，也就是 go 关键字后边的函数的地址。至此，一个可用的 goroutine 就创建好了！


// adjust Gobuf as if it executed a call to fn with context ctxt
// and then stopped before the first instruction in fn.
func gostartcall(buf *gobuf, fn, ctxt unsafe.Pointer) {
	sp := buf.sp // newg 的栈顶
	sp -= goarch.PtrSize // 栈顶向下移动 8 字节，用来存 return address
	*(*uintptr)(unsafe.Pointer(sp)) = buf.pc // return address =  goexit 函数的第二条指令的地址。也就是将return address 存储到了栈中
	buf.sp = sp // 设置 buf.sp 指向新的栈顶
	buf.pc = uintptr(fn) // buf.pc 执行函数地址 fn，后边 g 被调度起来，会从这里开始执行
	buf.ctxt = ctxt
}
*/
