package raft

import (
	"time"
)

const (
	//ms
	heartBeatWaitTime = 150
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int        // Leader term
	LeaderId     int        // Id of leader so follower can redirect
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prev log index
	Entries      []LogEntry //entry to store, empty for heartbeat
	LeaderCommit int        // leader commit index
}

type AppendEntriesReply struct {
	Term             int //currentTerm, for leader to update itself
	Success          bool
	CorrectNextIndex int //optimization for next filled only if reply is false due to log inconsistency
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.logMutex.Lock()
	followerPrevLogEntry := rf.log[args.PrevLogIndex]
	followerLogSize := len(rf.log)
	rf.logMutex.Unlock()

	reply.Success = false
	reply.CorrectNextIndex = 0

	if rf.killed() {
		return
	}

	//1
	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term

	rf.mu.Lock()
	rf.setFollower(args.Term)
	rf.mu.Unlock()

	//2
	//	Treat shorter log size of follower same as if entries does not match
	if followerLogSize >= args.PrevLogIndex && followerPrevLogEntry.Term != args.PrevLogTerm {
		reply.CorrectNextIndex = args.PrevLogIndex
		return
	}

	if args.PrevLogIndex > followerLogSize {
		reply.CorrectNextIndex = followerLogSize + 1
		return
	}

	reply.Success = true

	// write new entry to log in case this wasn't just heartbeat
	rf.mu.Lock()
	rf.logMutex.Lock()
	currentIndex := args.PrevLogIndex
	for _, entry := range args.Entries {
		currentIndex += 1
		followerLogSize = len(rf.log)

		//3
		// PROBLEM WHAT IF TERM IS DIFFERENT BUT SAME ENTRY AFTER REELECTION?

		if followerLogSize >= currentIndex && rf.log[currentIndex].Term != entry.Term {

			for i := currentIndex; i <= followerLogSize; i++ {

				delete(rf.log, i)
			}
		}

		//4
		// check if index and term matches to ensure coherency
		//append new entry
		rf.log[currentIndex] = entry

	}
	rf.logMutex.Unlock()
	rf.mu.Unlock()
	//PERSIST
	rf.persist()

	// 5
	if args.LeaderCommit > rf.commitIndex {
		rf.mu.Lock()
		if len(rf.log) < args.LeaderCommit {
			rf.commitIndex = len(rf.log)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.mu.Unlock()

		go notifyChannel(&rf.commitChannel, &rf.waitingToCommit)
	}

	//EXPERIMENTAL
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == nil {
		return true
	}
	return false
}

// spawned for each server on leader, sending appendentries to synchronize logs
func (rf *Raft) sync(server int) {

	// if you get reply with higher term then stop and become follower
	for !rf.killed() && rf.isLeader {

		// ok we will try to append at nextIndex if there are any new
		// and we will take value from leader log
		rf.mu.Lock()
		rf.logMutex.Lock()
		entries := []LogEntry{}
		for i := rf.nextIndex[server]; i <= len(rf.log); i++ {
			entry := rf.log[i]

			entries = append(entries, entry)
		}

		appendArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.logMutex.Unlock()
		rf.mu.Unlock()
		appendReply := &AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, appendArgs, appendReply)
		if ok {

			// got reply from server with higher term, become follower
			// goroutines for other servers will eventualy caught up and return too
			if appendReply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.setFollower(appendReply.Term)
				rf.mu.Unlock()
				rf.persist()
				return
				//succesful update
			} else if appendReply.Success && appendReply.Term == rf.currentTerm {

				//if heartbeat won't change anything as len(entries) will be 0
				rf.replicaMutex.Lock()
				for i := rf.nextIndex[server]; i < rf.nextIndex[server]+len(entries); i++ {
					rf.replicatedCnt[i] += 1
				}
				rf.replicaMutex.Unlock()

				rf.mu.Lock()
				rf.nextIndex[server] += len(entries)
				rf.matchIndex[server] += len(entries)

				if len(entries) > 0 {
					go notifyChannel(&rf.commitChannel, &rf.waitingToCommit)
				}
				rf.mu.Unlock()
				// when appendentries consistency check fails decrement nextindex and try again
			} else if !appendReply.Success && appendReply.Term == rf.currentTerm {

				if appendReply.CorrectNextIndex > 0 {
					rf.mu.Lock()
					rf.nextIndex[server] = appendReply.CorrectNextIndex
					rf.mu.Unlock()
					// no reason to wait better to instantly send new message with correct log
					continue
				}
				//rf.nextIndex[server] -= 1
			}

		}
		time.Sleep(time.Millisecond * heartBeatWaitTime)
	}
}
