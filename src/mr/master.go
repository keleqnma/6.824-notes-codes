package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskStatus int

const (
	TaskStatusReady TaskStatus = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinish
	TaskStatusErr
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 100
)

type TaskStat struct {
	Status    TaskStatus //task状态
	WorkerId  int        //处理该task的worker序号
	StartTime time.Time  //起始时间（用来计算有没有超时）
}

type Master struct {
	files      []string   //需要处理的files
	nReduce    int        //输入的参数nReduce（输入的文件会被划分成几个task来处理）
	taskPhase  TaskPhase  //taskPhase（map阶段还是reduce阶段）
	taskStats  []TaskStat //taskStats（各个task的状态）
	taskNum    int        //task数量
	mu         sync.Mutex //mu（全局锁）
	done       bool       //done（任务是否已完成）
	workerSeq  int        //workerSeq（有几个worker）
	taskCh     chan Task  //taskCh（用来分发task的channel）
	finishTask int32      //statCh（用来接受完成task数量）
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	DPrintf("Get task, taskseq:%d, len files:%d, len tasks:%d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

func (m *Master) taskSchedule(taskSeq int) {
	for {
		if m.Done() {
			return
		}
		returnStatus := false
		m.mu.Lock()
		DPrintf("Schedule begin, task:%v, Status: %v", taskSeq, m.taskStats[taskSeq].Status)
		switch m.taskStats[taskSeq].Status {
		case TaskStatusReady:
			m.taskCh <- m.getTask(taskSeq)
			m.taskStats[taskSeq].Status = TaskStatusQueue
		case TaskStatusQueue:
		case TaskStatusRunning:
			if time.Since(m.taskStats[taskSeq].StartTime) > MaxTaskRunTime {
				m.taskStats[taskSeq].Status = TaskStatusQueue
				m.taskCh <- m.getTask(taskSeq)
			}
		case TaskStatusFinish:
			atomic.AddInt32(&m.finishTask, 1)
			returnStatus = true
		case TaskStatusErr:
			m.taskStats[taskSeq].Status = TaskStatusQueue
			m.taskCh <- m.getTask(taskSeq)
		default:
			panic("Task status err")
			returnStatus = true
		}
		DPrintf("Schedule end, task:%v, Status: %v", taskSeq, m.taskStats[taskSeq].Status)
		m.mu.Unlock()
		if returnStatus {
			return
		}
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) initMapTask() {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("Init Map Task")
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
	m.taskNum = len(m.files)
	m.finishTask = 0
	for index := range m.taskStats {
		go m.taskSchedule(index)
	}
}

func (m *Master) initReduceTask() {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("Init Reduce Task")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
	m.taskNum = m.nReduce
	m.finishTask = 0
	for index := range m.taskStats {
		go m.taskSchedule(index)
	}
}

func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("Task phase doesn't match")
	}

	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	DPrintf("Get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("Get report task: %+v, taskPhase: %+v", args, m.taskPhase)

	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workerSeq++
	reply.WorkerId = m.workerSeq
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		DPrintf("Global schedule, finTask:%v, taskNum:%v\n", m.finishTask, m.taskNum)
		m.mu.Lock()
		if atomic.LoadInt32(&m.finishTask) == int32(m.taskNum) {
			if m.taskPhase == MapPhase {
				m.mu.Unlock()
				m.initReduceTask()
				time.Sleep(ScheduleInterval)
				continue
			} else {
				m.done = true
			}
		}
		m.mu.Unlock()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := &Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	DPrintf("Master init")
	return m
}
