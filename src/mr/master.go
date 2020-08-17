package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    TaskStatus //task状态
	WorkerId  int        //处理该task的worker序号
	mu        sync.Mutex //分段锁
	StartTime time.Time  //起始时间（用来计算有没有超时）
}

type Master struct {
	files     []string   //需要处理的files
	nReduce   int        //输入的参数nReduce（输入的文件会被划分成几个task来处理）
	taskPhase TaskPhase  //taskPhase（map阶段还是reduce阶段）
	taskStats []TaskStat //taskStats（各个task的状态）
	mu        sync.Mutex //mu（全局锁）
	done      bool       //done（任务是否已完成）
	workerSeq int        //workerSeq（有几个worker）
	taskCh    chan Task  //taskCh（用来分发task的channel）
	statCh    chan bool  //statCh（用来接受各task状态的channel）
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
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

func (m *Master) taskSchedule(taskSeq int, wg *sync.WaitGroup) {
	if m.Done() {
		return
	}
	m.taskStats[taskSeq].mu.Lock()
	DPrintf("begin,task:%v, Status: %v", taskSeq, m.taskStats[taskSeq].Status)
	switch m.taskStats[taskSeq].Status {
	case TaskStatusReady:
		m.statCh <- false
		m.taskCh <- m.getTask(taskSeq)
		m.taskStats[taskSeq].Status = TaskStatusQueue
	case TaskStatusQueue:
		m.statCh <- false
	case TaskStatusRunning:
		m.statCh <- false
		if time.Now().Sub(m.taskStats[taskSeq].StartTime) > MaxTaskRunTime {
			m.taskStats[taskSeq].Status = TaskStatusQueue
			m.taskCh <- m.getTask(taskSeq)
		}
	case TaskStatusFinish:
		m.statCh <- true
	case TaskStatusErr:
		m.statCh <- false
		m.taskStats[taskSeq].Status = TaskStatusQueue
		m.taskCh <- m.getTask(taskSeq)
	default:
		m.statCh <- false
		panic("t.status err")
	}
	defer m.taskStats[taskSeq].mu.Unlock()
	defer wg.Done()
}

func (m *Master) initMapTask() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
	for index := range m.taskStats {
		m.taskStats[index].mu = sync.Mutex{}
	}
}

func (m *Master) initReduceTask() {
	m.mu.Lock()
	defer m.mu.Unlock()
	DPrintf("init ReduceTask")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
	for index := range m.taskStats {
		m.taskStats[index].mu = sync.Mutex{}
	}
}

func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.taskStats[task.Seq].mu.Lock()
	defer m.taskStats[task.Seq].mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("req Task phase neq")
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
	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.taskStats[args.Seq].mu.Lock()
	defer m.taskStats[args.Seq].mu.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, m.taskPhase)

	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	go m.tickSingleTimer()
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
	//l, e := net.Listen("tcp", ":1234")
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
		m.tickSingleTimer()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) tickSingleTimer() {
	allFinish := true
	var wg sync.WaitGroup
	wg.Add(len(m.taskStats))
	for index := range m.taskStats {
		go m.taskSchedule(index, &wg)
	}
	for range m.taskStats {
		finStat := <-m.statCh
		allFinish = allFinish && finStat
	}
	wg.Wait()
	if allFinish {
		if m.taskPhase == MapPhase {
			log.Println("map done")
			m.initReduceTask()
		} else {
			m.mu.Lock()
			m.done = true
			m.mu.Unlock()
		}
	}
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
		m.statCh = make(chan bool, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
		m.statCh = make(chan bool, len(m.files))
	}

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	DPrintf("master init")
	return &m
}
