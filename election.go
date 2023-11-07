package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/fatih/color"
)

const DEBUG_ELECTION = false
const (
	Follower  = "follower"
	Leader    = "leader"
	Candidate = "candidate"

	electionTimeLow  = 400
	electionTimeHigh = 800
)

type Vote struct {
	Votedfor  int
	VotedTerm int
}

type RequestVoteArgs struct {
	Term         int // Candidate term
	CandidateId  int // Candidate ID
	LastLogIndex int // Index of cndidate last log entry
	LastLogTerm  int // term of candidate last log entry
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) waitElectionTime() {
	time.Sleep(time.Duration(rf.last_heard + rf.my_election_Time - time.Now().UnixMilli()))
}

func (rf *Raft) setFollower(correctTerm int) {
	if DEBUG_ELECTION {
		color.Magenta("MY ID IS %v I WAS TURNED INTO FOLLOWER :O GOT MESSAGE FROM TERM : %v, WHILE MINE WAS %v", rf.me, correctTerm, rf.currentTerm)
	}
	rf.state = Follower
	rf.currentTerm = correctTerm
	rf.my_election_Time = rand.Int63n(electionTimeHigh-electionTimeLow) + electionTimeLow
	rf.last_heard = time.Now().UnixMilli()
	rf.isLeader = false
}

// ok so this is handled on server that received request
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.logMutex.Lock()
	lastLogIndex := len(rf.log) // since we count indexes from 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.logMutex.Unlock()
	// vote is already granted,
	//	log isn't up to date enough
	// or current term is bigger than candidate

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.killed() || args.Term < rf.currentTerm {
		return
	}
	reply.Term = args.Term

	rf.mu.Lock()
	rf.setFollower(args.Term)
	rf.mu.Unlock()
	//deny vote if candidate has more incomplete log 30:46

	rf.mu.Lock()
	if (rf.vote.VotedTerm < args.Term && rf.currentTerm == args.Term) || args.Term > rf.currentTerm || (rf.vote.VotedTerm == args.Term && rf.vote.Votedfor == args.CandidateId) {
		//check if candidate log is atleast up to date
		if args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.vote = Vote{Votedfor: args.CandidateId, VotedTerm: args.Term}
			reply.VoteGranted = true
		}
	}
	rf.mu.Unlock()
	rf.persist()

	//Experimental
	reply.Term = args.Term
}

// Send request to all the other servers
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if time.Now().UnixMilli() > rf.last_heard+rf.my_election_Time && rf.state == Follower {
			if DEBUG_ELECTION {
				color.Magenta("////////////////////////////////////////////")
				color.Magenta("Hadn't Hear from leader, starting election")
				color.Magenta("MY INDEX: %v MY TERM WILL BE: %v", rf.me, rf.currentTerm+1)
				color.Magenta("////////////////////////////////////////////")
			}
			rf.startElection()

		} else {
			//	rf.my_election_Time = rand.Int63n(electionTimeHigh-electionTimeLow) + electionTimeLow
			rf.waitElectionTime()
			//
		}
	}
}

func (rf *Raft) startElection() {
	mutex := &sync.Mutex{}
	votes := 1 //votes for itself

	//update term
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1 // increment current term
	if DEBUG_ELECTION {
		color.Green("////////////////////////////////////////////////////////////////////")
		color.Green("STARTING ELECTION, MY ID IS : %v, MY TERM IS :%v", rf.me, rf.currentTerm)
		color.Green("////////////////////////////////////////////////////////////////////")
	}
	rf.vote.VotedTerm = rf.currentTerm // vote for self
	rf.vote.Votedfor = rf.me
	rf.mu.Unlock()
	rf.persist()
	// send request vote to all other peers

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.logMutex.Lock()
				reqVote := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
					LastLogTerm:  rf.log[len(rf.log)].Term,
				}
				replyVote := &RequestVoteReply{}
				rf.logMutex.Unlock()
				ok := rf.sendRequestVote(i, reqVote, replyVote)
				if ok {
					if replyVote.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.setFollower(replyVote.Term)
						rf.mu.Unlock()
						rf.persist()
					} else if replyVote.VoteGranted && replyVote.Term == rf.currentTerm {
						mutex.Lock()
						votes += 1
						mutex.Unlock()
					}
				}
			}(i)
		}
	}

	// set timer for election
	rf.my_election_Time = rand.Int63n(electionTimeHigh-electionTimeLow) + electionTimeLow
	rf.last_heard = time.Now().UnixMilli()

	for time.Now().UnixMilli() < rf.last_heard+rf.my_election_Time {
		if rf.state == Follower { // Was set by rpc from leader
			return
		} else if votes > len(rf.peers)/2 {
			if DEBUG_LOGSYNC {
				color.Yellow("*****************************************************************")
				color.Yellow("Becoming leader\n")
				color.Yellow("%v, got : %v  votes, out of : %v peers, in: %v term.\n", rf.me, votes, len(rf.peers), rf.currentTerm)
				rf.PrintLog()
				color.Yellow("*****************************************************************")
			}
			rf.mu.Lock()
			rf.state = Leader
			rf.isLeader = true
			//leader keeps nextIndex for each follower
			// index of next log entry to send to that follower
			// initialized to (1 + leader next index)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.nextIndex[i] = len(rf.log) + 1
					rf.matchIndex[i] = 0
					go rf.sync(i)
				}
			}
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sync(i)
				}
			}
			//maybe you got uncommited entry in the log?
			// ok but if you win this will be appended after any other server will accepnt nonempty heartbeat
			// what if we stop before that? So better uncommented probably
			//	but it gives weird races
			// go notifyChannel(&rf.commitChannel, &rf.waitingToCommit)
			return
		}
	}
	if !rf.killed() {
		rf.startElection()

	}

}
