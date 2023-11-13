package raft

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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"encoding/gob"
	"net/rpc"

	"github.com/fatih/color"
)

const DEBUG = false

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []*rpc.Client // RPC end points of all peers
	persister *Persister    // Object to hold this peer's persisted state
	me        int           // this peer's index into peers[]
	dead      int32         // set by Kill()

	// state a Raft server must maintain.

	//extra state that i find helpful to keep
	last_heard       int64
	my_election_Time int64

	waitingToCommit int32
	isLeader        bool
	commitChannel   chan bool //channel to wait on for new index to commit

	state string // follower | candidate leader
	vote  Vote

	applyChannel chan ApplyMsg
	// Persistent state on all
	currentTerm int              // Last term server has seen, increased by elections
	log         map[int]LogEntry // commands for state machine
	logMutex    sync.Mutex       // lock to protect acces to log

	//& term when entry was received by leader, starts at 1

	// Volatile state on all
	commitIndex int // Index of highest log entry known to be commited, initialized to 0, incr mono
	lastApplied int // Index of highest log entry applied to state machine, initialized to 0, incr mono

	// Volatile state on leader (reinitialized after election)
	replicatedCnt map[int]int // how many servers replicated log entry
	replicaMutex  sync.Mutex  // lock to protect acces to replicatedCnt
	nextIndex     []int       //for each server, index of the next log entry to send to that server init to last log index +1
	matchIndex    []int       //for each server, index of highest log entry known to be replicated on server
	// initialized to 0, increases monotonically
}

func (rf *Raft) Kill() {
	//free routines from waiting on lock
	atomic.StoreInt32(&rf.dead, 1)
	go notifyChannel(&rf.commitChannel, &rf.waitingToCommit)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// use as separate go routine when you need to notify on channel without blocking function
func notifyChannel(channel *chan bool, busy *int32) {
	if atomic.LoadInt32(busy) == 0 {
		atomic.StoreInt32(busy, 1)
		*channel <- true
		atomic.StoreInt32(busy, 0)
	}
}

// for logging
func writeToFile(fname string, log string) {
	f, err := os.OpenFile(fname,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(log); err != nil {
		println(err)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.isLeader
}

func (rf *Raft) PrintLog() {
	color.Blue("Server: %v\n", rf.me)
	for index := 1; index <= len(rf.log); index++ {
		color.Blue("MY TERM : %v|INDEX: %v, COMMAND: %v TERM: %v| COMMIT_INDEX: %v|", rf.currentTerm, index, rf.log[index].Command, rf.log[index].Term, rf.commitIndex)
	}
	color.Blue("\n")

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.vote)
	rf.mu.Unlock()
	rf.logMutex.Lock()
	e.Encode(rf.log)
	rf.logMutex.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	currentTerm := 0
	votedFor := (Vote{})
	log := (map[int]LogEntry{})

	err := d.Decode(&currentTerm)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = d.Decode(&votedFor)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = d.Decode(&log)
	if err != nil {
		fmt.Println(err)
		return
	}

	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.vote = votedFor
	rf.log = log
	rf.mu.Unlock()

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) commit(from int, to int) {
	rf.logMutex.Lock()
	for i := from + 1; i <= to; i++ {
		if DEBUG_LOGSYNC {
			log := fmt.Sprintf("INDEX : %v TERM : %v COMMAND : %v\n", i, rf.log[i].Term, rf.log[i].Command)
			writeToFile("COMMITAPPLIER"+strconv.Itoa(rf.me), log)
		}
		command := rf.log[i].Command
		rf.applyChannel <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: i}
	}
	rf.logMutex.Unlock()
}

func (rf *Raft) CommitController() {
	prevcommitIndex := rf.commitIndex
	for !rf.killed() {

		if rf.isLeader {
			for i := len(rf.log); i > prevcommitIndex; i-- {
				rf.replicaMutex.Lock()
				iCount := rf.replicatedCnt[i]
				rf.replicaMutex.Unlock()
				rf.logMutex.Lock()
				iTerm := rf.log[i].Term
				rf.logMutex.Unlock()

				if iCount > len(rf.peers)/2 && iTerm == rf.currentTerm {
					if DEBUG_LOGSYNC {
						color.Yellow("LEADER HERE(%v) INDEX: %v  REPLICAATED ON : %v , OUT OF %v MACHINES, LOGLEN: %v, I %v, PREVCOMINDEX: %v", rf.me, i, rf.replicatedCnt[i], len(rf.peers), len(rf.log), i, prevcommitIndex)
					}

					rf.mu.Lock()
					rf.commitIndex = i
					rf.mu.Unlock()

					break
				}
			}
		}
		if (!rf.isLeader && rf.commitIndex > prevcommitIndex) || rf.isLeader {
			if DEBUG_LOGSYNC {
				color.Yellow("FOLLOWER HERE(%v), WILL TRY TO COMMIT FROM INDEX %v TO INDEX %v", rf.me, prevcommitIndex, rf.commitIndex)
			}
			rf.mu.Lock()
			rf.commit(prevcommitIndex, rf.commitIndex)
			prevcommitIndex = rf.commitIndex
			rf.mu.Unlock()
			<-rf.commitChannel
		}

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// idk raft paper suggest sending valid leader in that case
	if !rf.isLeader {
		return -1, -1, false
	}

	//46:56 check if command already in log
	rf.logMutex.Lock()

	for idx, entry := range rf.log {
		if entry.Command == command {
			rf.logMutex.Unlock()
			return idx, entry.Term, true
		}
	}

	newEntry := LogEntry{Command: command, Term: rf.currentTerm}

	// TODO write to its own log and update state
	index := len(rf.log) + 1
	//rf.logMutex.Unlock()

	rf.replicaMutex.Lock()
	rf.replicatedCnt[index] = 1
	rf.replicaMutex.Unlock()
	//	rf.logMutex.Lock()
	rf.log[index] = newEntry
	rf.logMutex.Unlock()
	rf.persist()

	if DEBUG_LOGSYNC {
		color.Cyan("____________________________________________________________________________")
		color.Blue("----------Leader: %v got client reqeuset: %v----------", rf.me, command)
		color.Cyan("Leader log:")
		rf.PrintLog()
		color.Cyan("____________________________________________________________________________")
	}

	return index, rf.currentTerm, true
}

// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*rpc.Client, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.isLeader = false
	rf.my_election_Time = rand.Int63n(electionTimeHigh-electionTimeLow) + electionTimeLow
	rf.last_heard = time.Now().UnixMilli()

	rf.state = Follower
	//create buffered channel
	rf.commitChannel = make(chan bool, 1)

	rf.waitingToCommit = 0
	rf.currentTerm = 0
	rf.vote = Vote{Votedfor: -1, VotedTerm: -1}

	rf.applyChannel = applyCh

	rf.log = make(map[int]LogEntry)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.replicatedCnt = make(map[int]int)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitController()

	return rf
}
