# 2020 Spring 6.824 Lab1: MapReduce笔记

本节lab的代码：https://github.com/keleqnma/6.824-notes-codes/tree/master/src/mr

## 0. MapReduce架构

- 集群中的角色分类
  - Master 负责任务调度(分配任务，重新执行，调度等)
  - Worker 负责运行 Map 任务 或者 Reduce 任务
- worker 运行的任务分类
  - Map 任务： 每个Map 任务读取部分输入 产生中间的*k v* 数据
  - Reduce 任务： 读取map 产生的中间 *k v* 数据每个Reduce 产出一个输出文件

![image](https://raw.githubusercontent.com/phantooom/blog/master/image/MapReduce/01.png)

`MapReduce` 的整体思想是： **将输入的数据分成 M 个 `tasks`， 由用户自定义的 `Map` 函数去执行任务，产出 `<Key, Value>`形式的中间数据，然后相同的 key 通过用户自定义的 `Reduce` 函数去聚合，得到最终的结果。** 



1. MapReduce程序负责将用户的输入划分为M块 `16M ~ 64M` 的块大小。通过划分函数`(hash(key) mod R)` 会把Map中间数据划分为R个分区。
2. 将程序复制到集群中的各个需要运行的机器上并启动
3. Master 给空闲的机器分配Map 或者Reduce 任务，由于(1) 中说输入文件被划分了M块，分区函数 `mod R` 所以此时Map任务被划分为了M个任务，Reduce任务被划分了R个分区，同时最终结果也会产生 `<= R` 个最终输出的文件
4. 执行Map任务的worker读取相应的输入块,解析后发送给用户自定义的Map程序，用户Map程序将处理后的中间结果保存在内存当中。
5. 保存在内存中的中间结果会定期的被根据分区函数划分为`R个区域`写入本地磁盘，本地磁盘保存的`位置信息`会被传输到Master，Master将这些partation位置信息`转发`到Reduce 的worker。
6. Reduce worker 接收到这些位置信息后会通过`RPC调用`从Map Worker的磁盘中读取相应partation的中间结果，当Reduce读取了所有的中间结果的之后将`按照key进行一次排序`，因为多个worker任务产生的中间结果会被同一个Reduce worker 读取,所以为了保证结果有序还需要重新排序一次。
7. reduce worker 遍历排序过的中间数据，给每个遇到的唯一的中间key，将这个key和对应的value传递到用户的reduce 方法中。reduce 方法的输出会被添加到这个分区最终输出文件中。
8. 所有任务结束后会产生`R`个输出文件，不需要合并。

### Map 过程

- 根据输入输入信息，将输入数据 split 成 M 份， 比如上图中的 split0 - split4 `(这里M=5)`
- 在所有可用的`worker`节点中，起 M 个`task`任务的线程， 每个任务会读取对应一个 split 当做输入。
- 调用 `map` 函数，将输入转化为  `<Key, Value>` 格式的中间数据，并且排序后，写入磁盘。 这里，每个 `task` 会写 R 个文件，对应着 `Reduce` 任务数。 数据写入哪个文件的规则由 `Partitioner` 决定，默认是 `hash(key) % R`
- (可选) 为了优化性能，中间还可以用一个 `combiner` 的中间过程

### Reduce 过程

- `map` 阶段结束以后， 开始进入 `Reduce` 阶段，每个 `Reduce task`会从所有的 `Map` 中间数据中，获取属于自己的一份数据，拿到所有数据后，一般会进行排序`(Hadoop 框架是这样做)`  。

> 说明： 这个排序是非常必要的，主要因为 `Reduce` 函数的输入 是  `<key, []values>`  的格式，因为需要根据key去排序。有同学想为啥不用 `map<>()` 去实现呢？ 原因：因为map必须存到内存中，但是实际中数据量很大，往往需要溢写到磁盘。 但是排序是可以做到的，比如归并排序。 这也就是map端产出数据需要排序，Reduce端获取数据后也需要先排序的原因。

- 调用 `Reduce` 函数，得到最终的结果输出结果，存入对应的文件
- (可选) 汇总所有 `Reduce`任务的结果。一般不做汇总，因为通常一个任务的结果往往是另一个 `MapReduce`任务的输入，因此没必要汇总到一个文件中。
  

## 1.Map结构

`master` 是 `MapReduce` 任务中最核心的角色，它需要维护 **状态信息** 和 **文件信息**。

- 状态信息：
  - `map` 任务状态
  - `Reduce` 任务状态
  - `worker` 节点状态
- 文件信息
  - 输入文件信息
  - 输出文件信息
  - `map`中间数据文件信息

## 2. 容错

### worker 节点失败

`master`会周期性向所有节点发送`ping `心跳检测， 如果超时未回复，`master`会认为该`worker`已经故障。任何在该节点完成的`map `或者`Reduce`任务都会被标记为`idle`， 并由其他的`worker` 重新执行。

> 说明： 因为`MapReduce` 为了减少网络带宽的消耗，`map`的数据是存储在本地磁盘的，如果某个`worker`机器故障，会导致其他的`Reduce` 任务拿不到对应的中间数据，所以需要重跑任务。那么这也可以看出，如果利用`hadoop` 等分布式文件系统来存储中间数据，其实对于完成的`map`任务，是不需要重跑的，代价就是增加网络带宽。

### Master 节点失败

`master`节点失败，在没有实现HA 的情况下，可以说基本整个`MapReduce`任务就已经挂了，对于这种情况，直接重新启动`master` 重跑任务就ok了。 当然啦，如果集群有高可靠方案，比如`master`主副备用，就可以实现`master`的高可靠，**代价就是得同步维护主副之间的状态信息和文件信息等。**

### 失败处理

论文中提到，只要`MapReduce`函数是确定的，语义上不管是分布式执行还是单机执行，结果都是一致的。每个`map` `Reduce` 任务输出是通过原子提交来保证的， 即：

**一个任务要么有完整的最终文件，要么存在最终输出结果，要么不存在。**

- 每个进行中的任务，在没有最终语义完成之前，都只写临时文件，每个`Reduce` 任务会写一个，而每个`Map` 任务会写 R 个，对应 R 个`reducer`.
- 当 `Map` 任务完成的时候，会向`master`发送文件位置，大小等信息。`Master`如果接受到一个已经完成的`Map`任务的信息，就忽略掉，否则，会记录这个信息。
- 当 `Reduce` 任务完成的时候，会将临时文件重命名为最终的输出文件， 如果多个相同的`Reduce`任务在多台机器执行完，会多次覆盖输出文件，这个由底层文件系统的`rename`操作的原子性，保证任何时刻，看到的都是一个完整的成功结果

对于大部分确定性的任务，不管是分布式还是串行执行，最终都会得到一致的结果。对于不确定的`map` 或者`Reduce` 任务，`MapReduce` 保证提供一个弱的，仍然合理的语义。

> 举个例子来说:
>
> 确定性任务比如 词频统计   不管你怎么执行，串行或者并行，最终得到的都是确定性的统计结果。
>
> 第二个不确定性任务： 随机传播算法，`pageRank` 等，因为会有概率因素在里面，也就是说你每次跑的结果数据不一定能对的上。但是是合理的，因为本来就有很多随机的因素在里面。

## 3. 优化

### 存储优化

​	由于网络带宽资源的昂贵性，因此对`MapReduce`  存储做了很多必要的优化。

- 通过从本地磁盘读取文件，节约网络带宽
- GFS 将文件分解成多个 大小通常为 64M 的`block`, 并多备份存储在不同的机器上，在调度时，会考虑文件的位置信息，尽可能在存有输入文件的机器上调度`map`任务，避免网络IO。
- 任务失败时，也会尝试在离副本最近的worker中执行，比如同一子网下的机器。
- MapReduce 任务在大集群中执行时，大部分输入直接可以从磁盘中读取，不消耗带宽。

### 任务粒度

通常情况下，任务数即为 `O(M + R)`,  这个数量应当比`worker`数量多得多，这样利于负载均衡和失败恢复的情况，但是也不能无限增长，因为太多任务的调度，会消耗`master` 存储任务信息的内存资源，如果启动task所花的时间比任务执行时间还多，那就不偿失了。

#### 自定义分区函数 (`partition`)：

自定义分区可以更好地符合业务和进行负载均衡，防止数据倾斜。 默认只是简单的 `hash(key) % R`

#### 有序保证：

每个`partition`内的数据都是排序的，这样有利于`Reduce`阶段的`merge`合并

#### `Combiner` 函数：

这个是每个`map`阶段完成之后，局部先做一次聚合。比如：词频统计，每个 Word 可能出现了100次，如果不使用`combiner`， 就会发送100 个 `<word, 1>`, 如果`combiner`聚合之后，则为 `<word, 100>`, 大大地减少了网络传输和磁盘的IO。

#### 输入输出类型

一个`reader`没必要非要从文件读数据，`MapReduce` 支持可以从不同的数据源中以多种不同的方式读取数据，比如从数据库读取，用户只需要自定义split规则，就能轻易实现。

#### 计数器

`MapReduce` 还添加了计数器，可以用来检测`MapReduce`的一些中间操作。

## 4. 实现

### 初始代码逻辑

#### 1. MapReduce应用

`mrapps`文件夹里包含了很多`mapreduce`应用，比如wc.go(wordcount)，用来数单词频率的，每个应用都定义了自己的map函数和reduce函数，这里看一下wc.go里这两个函数的定义：

```go
func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
```

可以看到，定义很简单，map函数输出<word, '1'>，reduce函数输入聚合后众多个<word,'1'>，输出'1'的长度，即该单词出现的总次数。

将这个应用定义的函数和函数导出：

> go build -buildmode=plugin ../mrapps/wc.go

**解释：**plugin（插件）模式会把该文件的函数和变量导出到`.so`文件，其他文件可以通过引用`plugin`库来调用，可以看这里：https://medium.com/learning-the-go-programming-language/writing-modular-go-programs-with-plugins-ec46381ee1a9

#### 2. MapReduce过程

随后，启动sequential（串行，非并行）的示例：

> go run mrsequential.go wc.so pg*.txt

```go
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

  // 示例中，os.Args[1] = wc.so, 读取wc.so中定义的map函数和reduce函数，赋值给mapf和recudef变量
	mapf, reducef := loadPlugin(os.Args[1])

	// Map过程，输出多个文件的map结果
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	// 
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
  
  // 将中间结果排序
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	// 
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
    //将相同的key找出来（这也是排序的意义）
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
    //将拥有相同的key的键值对合并
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
    //输入到reduce函数里，得到输出
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}
```

这个示例演示了一个基础的MapReduce流程是怎样的。

### 自己写代码

下来我们开始写代码吧！根据官方指引：

> One way to get started is to modify `mr/worker.go`'s `Worker()` to send an RPC to the master asking for a task. Then modify the master to respond with the file name of an as-yet-unstarted map task. Then modify the worker to read that file and call the application Map function, as in `mrsequential.go`.

我们来分析一下逻辑，在已经给出的串行MapReduce中，单一进程按照顺序执行Map任务和Reduce任务，但是在要实现的并行MapReduce中，我们将启动一个Master和多个Worker。

RPC教程可以看这里：https://golang.org/pkg/net/rpc/，简单来说就是通过注册对象来调用远程服务端的函数。

#### 1.数据结构分析

```go
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

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int
	Phase    TaskPhase
	Alive    bool // worker should exit when alive is false
}
```

#### 2.调用逻辑

起始Master初始化后，后续启动的woker进程则会通过调用`RegWorker`在Master进程注册：

```go
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq++
	reply.WorkerId = m.workerSeq
	return nil
}
```

随后woker进程会调用`GetOneTask`请求Master分配任务，Master会从taskChannel里获取一个task并初始化Task：

```go
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
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
```

获取Task之后，Woker进程根据Task的Phase不同分别进行不同的处理：

```go
func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)

}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail:%+v", args)
	}
}
```

完成任务后，Worker进程向Master进程汇报，并重新循环请求新任务，Master进程判断当前任务的合法性以及是否正常完成，如果正常结束则启动一次单次全局调度来刷新状态：

```go
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
```

#### 3.Master调度过程

在Worker进程在处理任务时，Master进程也在进行调度：

```go
//只要任务没有完成结束就定期启用调度
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
		go m.taskSchedule(index, &wg) //对每个taskstate都启用单独的goroutine调度
	}
	for range m.taskStats {
		finStat := <-m.statCh//从信道中读取状态
		allFinish = allFinish && finStat
	}
	wg.Wait()//等待goroutines都结束（不然后面更新phase的时候全局锁不覆盖局部锁就会产生竞争）
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

func (m *Master) taskSchedule(taskSeq int, wg *sync.WaitGroup) {
	if m.Done() {
		return
	}
	m.taskStats[taskSeq].mu.Lock()
	DPrintf("begin,task:%v, Status: %v", taskSeq, m.taskStats[taskSeq].Status)
	switch m.taskStats[taskSeq].Status {
	case TaskStatusReady://初始状态，将其放入task channel
		m.statCh <- false
		m.taskCh <- m.getTask(taskSeq)
		m.taskStats[taskSeq].Status = TaskStatusQueue
	case TaskStatusQueue://排队中，未被worker领取
		m.statCh <- false
	case TaskStatusRunning://正在被worker处理，判断一下时间有没有超时
		m.statCh <- false
		if time.Now().Sub(m.taskStats[taskSeq].StartTime) > MaxTaskRunTime {
			m.taskStats[taskSeq].Status = TaskStatusQueue
			m.taskCh <- m.getTask(taskSeq)
		}
	case TaskStatusFinish://正常结束的task
		m.statCh <- true
	case TaskStatusErr://错误结束的task，将其重新放入队列中
		m.statCh <- false
		m.taskStats[taskSeq].Status = TaskStatusQueue
		m.taskCh <- m.getTask(taskSeq)
	default://异常状态
		m.statCh <- false
		panic("t.status err")
	}
	defer m.taskStats[taskSeq].mu.Unlock()
	defer wg.Done()
}
```



以上就是MapReduce我个人的一些心得了。

代码参考：

1. [https://titanssword.github.io/2018-01-20-mapreduce%20implements.html](https://titanssword.github.io/2018-01-20-mapreduce implements.html)
2. https://github.com/kophy/6.824





