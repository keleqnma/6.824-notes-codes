package raft

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		_, path, lineno, ok := runtime.Caller(1)
		_, file := filepath.Split(path)

		if ok {
			t := time.Now()
			a = append([]interface{}{t.Format("2006-01-02 15:04:05.00"), file, lineno}, a...)
			fmt.Printf("%s [%s:%d] "+format+"\n", a...)
		}
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
