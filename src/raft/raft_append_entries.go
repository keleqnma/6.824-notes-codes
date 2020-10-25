package raft

import (
	"math/rand"
	"time"
)

const (
	appendEntriesStageOne = "appendEnties:stage:sendRPC"
	appendEntriesStageTwo = "appendEnties:stage:commit&snapshot"
)

func randRPCTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % RPCTimeout
	return RPCTimeout + r
}

func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

// 如果要添加的log比当前服务器存储log的index小，而且term还没有变化（leader没有变化），说明leader发送了不必要的appendEntries
func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("append_entries")
	rf.log("get appendentries:%+v", *args)
	reply.Term = rf.currentTerm

	// 如果发送方的term小于当前term，拒绝添加
	if rf.currentTerm > args.Term {
		rf.unlock("append_entries")
		return
	}

	// 接收到appendentries后重置选举计时器，并且重置角色
	rf.currentTerm = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.lastLogTermIndex()

	switch {
	case args.PrevLogIndex > lastLogIndex:
		// 缺少中间的 log
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	case args.PrevLogIndex == rf.lastSnapshotIndex:
		// TODO 重复代码
		// 上一个刚好是快照
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:1], args.Entries...) // 保留 logs[0]
			reply.NextIndex = rf.getNextIndex()
		}
	case rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm:
		// 包括刚好是后续的 log 和需要删除部分 两种情况
		// 乱序的请求返回失败
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[0:rf.getRealIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	default:
		rf.log("prev log not match")
		reply.Success = false
		// 尝试跳过一个 term
		term := rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRealIdxByLogIndex(idx)].Term == term {
			idx -= 1
		}
		// 这里没有让leader采取回退政策，而是直接计算告诉leader下一次应该从哪里发送日志
		reply.NextIndex = idx + 1
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	rf.log("get appendentries:%+v, reply:%+v", *args, *reply)
	rf.unlock("append_entries")
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if nextIdx <= rf.lastSnapshotIndex || nextIdx > lastLogIndex {
		// 没有需要发送的 log
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = lastLogTerm
		return args
	}

	args.Entries = append([]LogEntry{}, rf.logEntries[rf.getRealIdxByLogIndex(nextIdx):]...)
	args.PrevLogIndex = nextIdx - 1
	if args.PrevLogIndex == rf.lastSnapshotIndex {
		args.PrevLogTerm = rf.lastSnapshotTerm
	} else {
		args.PrevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
	}
	return args
}

func (rf *Raft) resetAllHeartBeatTimers() {
	for peerIdx := range rf.appendEntriesTimers {
		rf.resetHeartBeatTimer(peerIdx)
	}
}

func (rf *Raft) resetHeartBeatTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HeartBeatTimeout)
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	rf.appendEnriesMu[peerIdx].Lock()
	RPCTimer := time.NewTimer(randRPCTimeout())
	defer RPCTimer.Stop()
	defer rf.appendEnriesMu[peerIdx].Unlock()
RPCLoop:
	for !rf.killed() {
		rf.resetHeartBeatTimer(peerIdx)
		if rf.role != Leader {
			return
		}

		rf.lock(appendEntriesStageOne)
		args := rf.getAppendEntriesArgs(peerIdx)
		rf.unlock(appendEntriesStageOne)

		RPCTimer.Stop()
		RPCTimer.Reset(randRPCTimeout())
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)

		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
		RPCSleepLoop:
			for !ok {
				select {
				case <-RPCTimer.C:
					break RPCSleepLoop
				default:
					//if no reply receive, wait
					time.Sleep(time.Millisecond * 30)
				}
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			rf.log("append to peer, rpctimeout: peer:%d, args:%+v", peerIdx, args)
			continue RPCLoop
		case ok := <-resCh:
			if !ok {
				rf.log("append to peer no reply, peer:%d, args:%+v", peerIdx, args)
				continue RPCLoop
			}
		}

		rf.log("append to perr, peer:%d, args:%+v, reply:%+v", peerIdx, args, reply)

		rf.lock(appendEntriesStageTwo)

		// 失败情况 1. 目标节点的term比现在leader高（可能是因为网络隔离导致的）
		if reply.Term > rf.currentTerm {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			rf.unlock(appendEntriesStageTwo)
			return
		}

		// 失败情况 2. 现在节点已不再是leader，或者节点term发生了变化
		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.unlock(appendEntriesStageTwo)
			return
		}

		if reply.Success {
			// 同步一下已经复制的位置
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			// 同步log成功，更新一下
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				// 只 commit 自己 term 的 index
				rf.updateCommitIndex()
			}
			rf.persist()
			rf.unlock(appendEntriesStageTwo)
			return
		}

		// success == false
		if reply.NextIndex != 0 {
			if reply.NextIndex > rf.lastSnapshotIndex {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.unlock(appendEntriesStageTwo)
				continue RPCLoop
			} else {
				// send sn rpc
				go rf.sendInstallSnapshot(peerIdx)
				rf.unlock(appendEntriesStageTwo)
				return
			}
		}
		rf.unlock(appendEntriesStageTwo)
	}

}

func (rf *Raft) updateCommitIndex() {
	rf.log("in update commitindex")
	hasCommit := false
	for logIndex := rf.commitIndex + 1; logIndex <= rf.lastSnapshotIndex+len(rf.logEntries); logIndex++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= logIndex {
				count += 1
				// 一个log被超过半数的节点成功写入，则可以判断是成功写入了
				if count > len(rf.peers)/2 {
					rf.commitIndex = logIndex
					hasCommit = true
					rf.log("update commit index:%d", logIndex)
					break
				}
			}
		}
		if rf.commitIndex != logIndex {
			// 后续的不需要再判断
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}
