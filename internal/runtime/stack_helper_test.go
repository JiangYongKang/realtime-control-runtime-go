package runtime

import "runtime"

func stackCapture(buf []byte) int {
	runtime.GC()
	return runtime.Stack(buf, true)
}
