package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rand.Seed(time.Now().Unix())
}

const (
	ElectionInterval = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 150 // leader 发送心跳
	ApplyInterval    = time.Millisecond * 100
	RPCTimeout       = time.Millisecond * 100
	MaxLockTime      = time.Millisecond * 10
	voteForNobody    = -1
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer
	notifyApplyCh       chan struct{}
	stopCh              chan struct{}

	voteFor     int        // server id, -1 for null
	logEntries  []LogEntry // 日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）,lastSnapshot 放到 index 0
	applyCh     chan ApplyMsg
	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	//leader状态
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	lastSnapshotIndex int // 快照中的 index
	lastSnapshotTerm  int

	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.lock("get state")
	defer rf.unlock("get state")
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(rf.currentTerm); err != nil {
		rf.log("encode term error:%v", err)
	}
	if err := e.Encode(rf.voteFor); err != nil {
		rf.log("encode votefor error:%v", err)
	}
	if err := e.Encode(rf.commitIndex); err != nil {
		rf.log("encode commitIndex error:%v", err)
	}
	if err := e.Encode(rf.lastSnapshotIndex); err != nil {
		rf.log("encode lastSnapshotIndex error:%v", err)
	}
	if err := e.Encode(rf.lastSnapshotTerm); err != nil {
		rf.log("encode lastSnapshotTerm error:%v", err)
	}
	if err := e.Encode(rf.logEntries); err != nil {
		rf.log("encode logEntries error:%v", err)
	}
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.log("begin persist")
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var logs []LogEntry
	var commitIndex, lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&logs) != nil {
		rf.log("rf read persist err")
	} else {
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logEntries = logs
	}

}

func (rf *Raft) lock(m string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = m
}

func (rf *Raft) unlock(m string) {
	rf.lockEnd = time.Now()
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		rf.log("lock too long:%s:%s:iskill:%v", m, duration, rf.killed())
	}
	rf.mu.Unlock()
}

func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
		rf.log("change to follower")
	case Candidate:
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.resetElectionTimer()
		rf.log("change to candidate")
	case Leader:
		_, lastLogIndex := rf.lastLogTermIndex()
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
		rf.log("change to leader")
	default:
		panic("unknown role")
	}

}

// 返回最后一条log的term和index
func (rf *Raft) lastLogTermIndex() (lastLogTerm int, lastLogIndex int) {
	lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
	lastLogIndex = rf.lastSnapshotIndex + len(rf.logEntries) - 1
	return
}

func (rf *Raft) getLogByIndex(logIndex int) LogEntry {
	idx := logIndex - rf.lastSnapshotIndex
	return rf.logEntries[idx]
}

func (rf *Raft) getRealIdxByLogIndex(logIndex int) int {
	idx := logIndex - rf.lastSnapshotIndex
	if idx < 0 {
		return -1
	} else {
		return idx
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.lock("start")
	term := rf.currentTerm
	_, lastIndex := rf.lastLogTermIndex()
	index := lastIndex + 1

	isLeader := (rf.role == Leader)
	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
			Idx:     index,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	rf.resetHeartBeatTimers()
	rf.unlock("start")
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) log(format string, a ...interface{}) {
	if Debug > 0 {
		if Debug > 0 {
			_, path, lineno, ok := runtime.Caller(1)
			_, file := filepath.Split(path)
			if ok {
				t := time.Now()
				a = append([]interface{}{t.Format("2006-01-02 15:04:05.00"), file, lineno}, a...)
				fmt.Printf("me: %d, role:%v, term:%d\n", rf.me, rf.role, rf.currentTerm)
				fmt.Printf("%s [%s:%d] "+format+"\n\n", a...)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash

	rf.stopCh = make(chan struct{})
	rf.currentTerm = 0
	rf.voteFor = voteForNobody
	rf.role = Follower
	rf.logEntries = make([]LogEntry, 1) // idx ==0 存放 lastSnapshot
	rf.readPersist(persister.ReadRaftState())

	rf.log("raft server %d begin initialization", me)

	// 发起投票
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	// leader 发送日志
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i := range rf.peers {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatTimeout)
	}
	for i := range peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[index].C:
					rf.appendEntriesToPeer(index)
				}
			}
		}(i)
	}

	rf.readPersist(persister.ReadRaftState())
	return rf
}
