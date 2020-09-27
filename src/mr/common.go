package mr

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
)

const Debug = true

func DPrintf(format string, v ...interface{}) {
	if Debug {
		_, path, lineno, ok := runtime.Caller(1)
		_, file := filepath.Split(path)

		if ok {
			t := time.Now()
			a := []interface{}{t.Format("2006-01-02 15:04:05.00"), file, lineno}
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

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int
	Phase    TaskPhase
	Alive    bool // worker should exit when alive is false
}
