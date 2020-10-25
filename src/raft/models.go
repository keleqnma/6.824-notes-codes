package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// leader用这个去replicate log entries, 也用于heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        //紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry //需要被保存的日志条目（被当做心跳使用时日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        //Ledaer已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term      int  // 当前服务器的term
	Success   bool // 是否成功
	NextIndex int  // 副本主动报告自己需要更新的log index， raft论文里说这种优化没必要，“在实践中，我们十分怀疑这种优化是否是必要的，因为失败是很少发生的并且也不大可能会有这么多不一致的日志。”，但是6.824的模拟环境里就是有这么多，所以需要实现这个优化。
}
