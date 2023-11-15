package raft

import (
	"time"

	"github.com/fatih/color"
)

const DEBUG_LOGSYNC = true
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
	if len(args.Entries) == 0 && DEBUG_LOGSYNC {
		color.Red("\n_____________________________________________________________________")
		color.Red("|----------Got HEARTBEAT :)----------------------------|")
		color.Red("MY ID IS : %v GOT APPEND ENTRY, TERM %v| LEADERID %v| PREVLOGINDEX %v| PREVLOGTERM %v| LEADERCOMMIT %v", rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		color.Magenta("MY LOG : ")
		rf.PrintLog()
		color.Red("|-------------------------------------------------------------------\n")
	}
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
		if DEBUG_LOGSYNC {

			color.Red("GOT HEARTBEAT WITH TERM %v WHILE MY IS %v", args.Term, rf.currentTerm)
		}
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

		if DEBUG_LOGSYNC {
			color.Red("|-------------------------------------------------------------------")
			color.Red("|------Append entries---------------------------------------")
			color.Red("MY ID IS : %v AND MY TERM IS: %v \nGOT APPEND ENTRY WITH: TERM %v| LEADERID %v| PREVLOGINDEX %v| PREVLOGTERM %v| LEADERCOMMIT %v", rf.me, rf.currentTerm, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			rf.PrintLog()
		}
		//3
		// PROBLEM WHAT IF TERM IS DIFFERENT BUT SAME ENTRY AFTER REELECTION?

		if followerLogSize >= currentIndex && rf.log[currentIndex].Term != entry.Term {
			if DEBUG_LOGSYNC {
				color.Red("WILL DELETE FEW ENTRIES")
				color.Red("Conflict at index: %v Current: %v %v New: %v %v", currentIndex, rf.log[currentIndex].Term, rf.log[currentIndex].Command, args.PrevLogTerm, entry.Command)
			}
			for i := currentIndex; i <= followerLogSize; i++ {
				if DEBUG_LOGSYNC {
					color.Red("DELETE AT INDEX : %v WITH DATA: %v %v", i, rf.log[i].Term, rf.log[i].Command)
				}
				delete(rf.log, i)
			}
		}

		//4
		// check if index and term matches to ensure coherency
		//append new entry
		rf.log[currentIndex] = entry
		if DEBUG_LOGSYNC {
			color.Red("|------New entry applied!---------------------------------------")
			color.Red("MY ID IS : %v GOT APPEND ENTRY, TERM %v| LEADERID %v| PREVLOGINDEX %v| PREVLOGTERM %v| ENTRY %v| LEADERCOMMIT %v", rf.me, args.Term, args.LeaderId, currentIndex-1, args.PrevLogTerm, entry, args.LeaderCommit)
			rf.PrintLog()
			color.Red("|-------------------------------------------------------------------")
		}

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

		if DEBUG_LOGSYNC {
			color.Yellow("APPENDENTRY, INDEX(%v) SENDING NOTIFICATION TO COMMIT", rf.me)
		}

		go notifyChannel(&rf.commitChannel, &rf.waitingToCommit)
	}

	//EXPERIMENTAL
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
			if DEBUG_LOGSYNC {
				color.Cyan("ENTRY : %v\n", entry)
			}

			entries = append(entries, entry)
		}

		if DEBUG_LOGSYNC {
			color.Green("___________________________________________________________________________________")
			color.Green("|-------Sync METHOD Of leader %v--------------------------------------------", rf.me)
			color.Green("NEXT INDEX OF THE SERVER IS :%v, LOG LEN IS %v", rf.nextIndex[server], len(rf.log))
			//color.Green("TO SERVER ID: %v CURRENT INDEX: %v, ENTRY: %v", server, newEntryIndex, rf.log[newEntryIndex].Command)
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

		if DEBUG_LOGSYNC {
			if len(entries) > 0 {
				color.Red("PREPARING TO SENT ENTRY TO SERVER :%v, TERM %v| LEADERID %v| PREVLOGINDEX %v| PREVLOGTERM %v| LEADERCOMMIT %v", server, appendArgs.Term, appendArgs.LeaderId, appendArgs.PrevLogIndex, appendArgs.PrevLogTerm, appendArgs.LeaderCommit)
			} else {
				color.Red("PREPARING TO SENT HEARTBEAT TO SERVER :%v, TERM %v| LEADERID %v| PREVLOGINDEX %v| PREVLOGTERM %v| LEADERCOMMIT %v", server, appendArgs.Term, appendArgs.LeaderId, appendArgs.PrevLogIndex, appendArgs.PrevLogTerm, appendArgs.LeaderCommit)

			}
		}
		ok := rf.sendAppendEntries(server, appendArgs, appendReply)
		if ok {
			if DEBUG_LOGSYNC {
				color.Green("METHOD SYNC INDEX %v GOT REPLY TERM %v AND SUCCES? : %v", server, appendReply.Term, appendReply.Success)
			}
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
				if DEBUG_LOGSYNC {
					color.Red("Got succesful reply from server %v", server)
					color.Green("___________________________________________________________________________________")
				}
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

				//color.Green("INDEX %v, FAILED MESSAGE WAS SENT BY SYNCLOGGER :)", server)
				if DEBUG_LOGSYNC {
					color.Red("Sending entry failed will try again with lower index")
					color.Green("___________________________________________________________________________________")
				}
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
