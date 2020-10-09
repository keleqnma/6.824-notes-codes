package raft

import (
	"math/rand"
	"time"
)

func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionInterval
	return ElectionInterval + r
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("req_vote")
	defer rf.unlock("req_vote")
	defer func() {
		rf.log("get request vote, req:%+v, reply:%+v", req, reply)
	}()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	switch {
	case req.Term == rf.currentTerm:
		return
	case req.Term < rf.currentTerm:
		if rf.role == Leader {
			return
		}
		if rf.voteFor == req.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.voteFor != voteForNobody && rf.voteFor != req.CandidateId {
			return
		}
	}

	defer rf.persist()

	if lastLogTerm > req.LastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex) {
		// 选取限制
		return
	}

	rf.currentTerm = req.Term
	rf.voteFor = req.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	rf.log("vote for:%d", req.CandidateId)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) sendRequestVoteToPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.lock("start_election")
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.unlock("start_election")
		return
	}
	rf.log("start election")
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.unlock("start_election")

	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(votesCh chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVoteToPeer(index, &req, &reply)
			votesCh <- reply.VoteGranted
			if reply.Term > req.Term {
				rf.lock("start_ele_change_term")
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start_ele_change_term")
			}
		}(votesCh, index)
	}

	for {
		chResCount += 1
		if <-votesCh {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		rf.log("grantedCount <= len/2, len:%d, count:%d", len(rf.peers), grantedCount)
		return
	}

	rf.lock("start_ele2")
	rf.log("before try change to leader, count:%d, args:%+v", grantedCount, req)
	if rf.currentTerm == req.Term && rf.role == Candidate {
		rf.changeRole(Leader)
		rf.persist()
	}

	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}

	rf.unlock("start_ele2")
}
