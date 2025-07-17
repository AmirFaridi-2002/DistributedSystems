// Refactored Raft implementation integrated with your logic.
// Follows template shape and fills with your working code.

package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"sort"
	"6.5840/labrpc"
	"6.5840/labgob"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)
import "encoding/gob"

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	VotedForNull = -1
)

type Raft struct {
	mu                sync.Mutex
	peers             []*labrpc.ClientEnd
	persister         *tester.Persister
	me                int
	dead              int32

	currentTerm       int
	votedFor          int
	log               []interface{} // each entry is map[string]interface{}

	commitIndex       int
	lastApplied       int

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	nextIndex         []int
	matchIndex        []int

	state             RaftState
	rng               *rand.Rand
	electionTimer     *time.Timer
	heartbeatInterval time.Duration
	applyCond         *sync.Cond
	applyCh           chan raftapi.ApplyMsg

	electionTimerReset time.Time

}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	var buf bytes.Buffer
	e := labgob.NewEncoder(&buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(buf.Bytes(), rf.snapshot)
}


func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, vote int
	var log []interface{}
	var lastIncludedIndex, lastIncludedTerm int

	if d.Decode(&term) == nil &&
	   d.Decode(&vote) == nil &&
	   d.Decode(&log) == nil &&
	   d.Decode(&lastIncludedIndex) == nil &&
	   d.Decode(&lastIncludedTerm) == nil {

		rf.currentTerm = term
		rf.votedFor = vote
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}


func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	offset := index - rf.lastIncludedIndex - 1
	var snapshotTerm int
	if offset >= 0 && offset < len(rf.log) {
		snapshotTerm = rf.log[offset].(map[string]interface{})["Term"].(int)
	} else if offset == len(rf.log) {
		//Edge case: snapshot index is exactly at last log index + 1. We assume the snapshot term is the same as last log entry
		if len(rf.log) > 0 {
			snapshotTerm = rf.log[len(rf.log)-1].(map[string]interface{})["Term"].(int)
		} else {
			snapshotTerm = rf.lastIncludedTerm
		}
	} else {//invalid request
		return
	}

	newLog := make([]interface{}, 0)
	if offset+1 < len(rf.log) {
		newLog = append(newLog, rf.log[offset+1:]...)
	}
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = snapshotTerm
	rf.snapshot = snapshot

	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.transitionTo(Follower, args.Term)
		reply.Term = rf.currentTerm
	}
	isLogFreshEnough := func() bool {
		myLastIdx, myLastTerm := rf.lastLog()
		switch {
		case args.LastLogTerm > myLastTerm:
			return true
		case args.LastLogTerm < myLastTerm:
			return false
		default: 
			return args.LastLogIndex >= myLastIdx
		}
	}()
	allowedByHistory := (rf.votedFor == VotedForNull) || (rf.votedFor == args.CandidateId)
	if allowedByHistory && isLogFreshEnough {
		rf.votedFor = args.CandidateId   
		rf.persist() 
		rf.resetElectionTimer() 
		reply.VoteGranted = true 
	}
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.lastIncludedIndex + len(rf.log) + 1
	rf.log = append(rf.log, map[string]interface{}{
		"Term":    rf.currentTerm,
		"Command": command,
	})
	rf.persist()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		timedOut := rf.state != Leader && time.Since(rf.electionTimerReset) >= rf.randomElectionTimeout()
		rf.mu.Unlock()
		if timedOut {
			rf.mu.Lock()
			rf.transitionTo(Candidate, rf.currentTerm+1)
			rf.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		votedFor:          VotedForNull,
		log:               make([]interface{}, 0),
		applyCh:           applyCh,
		state:             Follower,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		heartbeatInterval: 100 * time.Millisecond,
		electionTimer:     time.NewTimer(300 * time.Millisecond),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applyEntries()

	return rf
}


func (rf *Raft) transitionTo(state RaftState, term int) {
	rf.state = state
	rf.currentTerm = term

	switch state {
	case Follower:
		rf.votedFor = VotedForNull
	case Candidate:
		rf.votedFor = rf.me
	case Leader:
		lastIndex, _ := rf.lastLog()
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = lastIndex + 1
			rf.matchIndex[i] = 0
		}
	}

	// Inline persist
	var buf bytes.Buffer
	e := labgob.NewEncoder(&buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.Save(buf.Bytes(), rf.persister.ReadSnapshot())

	rf.resetElectionTimer()

	switch state {
	case Candidate:
		go rf.sendVotes()
	case Leader:
		go rf.heartbeatLoop()
	}
}

func (rf *Raft) lastLog() (int, int) {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	lastIndex := rf.lastIncludedIndex + len(rf.log)
	lastTerm := rf.log[len(rf.log)-1].(map[string]interface{})["Term"].(int)
	return lastIndex, lastTerm
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(150+rf.rng.Intn(150)) * time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	timeout := rf.randomElectionTimeout()
	rf.electionTimer.Reset(timeout)
	rf.electionTimerReset = time.Now()
}

func (rf *Raft) sendVotes() {
	rf.mu.Lock()
	term := rf.currentTerm
	lastIndex, lastTerm := rf.lastLog()
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.mu.Unlock()

	var votes int32 = 1
	majority := len(rf.peers)/2 + 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(p, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate || rf.currentTerm != term {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.transitionTo(Follower, reply.Term)
					return
				}
				if reply.VoteGranted {
					count := atomic.AddInt32(&votes, 1)
					if int(count) >= majority && rf.state == Candidate {
						rf.transitionTo(Leader, rf.currentTerm)
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) heartbeatLoop() {
	ticker := time.NewTicker(rf.heartbeatInterval)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.currentTerm
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.syncWithPeer(i, currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) syncWithPeer(peerIdx int, term int) {
	rf.mu.Lock()

	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peerIdx]
	isSnapshot := nextIdx <= rf.lastIncludedIndex

	var method string
	var args interface{}

	if isSnapshot {
		method = "Raft.InstallSnapshot"
		args = map[string]interface{}{
			"Term":              rf.currentTerm,
			"LeaderId":          rf.me,
			"LastIncludedIndex": rf.lastIncludedIndex,
			"LastIncludedTerm":  rf.lastIncludedTerm,
			"Data":              rf.snapshot,
		}
	} else {
		prevLogIndex := nextIdx - 1
		prevLogTerm := rf.lastIncludedTerm
		if prevLogIndex > rf.lastIncludedIndex {
			sliceIdx := prevLogIndex - rf.lastIncludedIndex - 1
			prevLogTerm = rf.log[sliceIdx].(map[string]interface{})["Term"].(int)
		}

		var entries []interface{}
		for i := nextIdx - rf.lastIncludedIndex - 1; i < len(rf.log); i++ {
			entries = append(entries, rf.log[i])
		}

		method = "Raft.AppendEntries"
		args = map[string]interface{}{
			"Term":         rf.currentTerm,
			"LeaderId":     rf.me,
			"PrevLogIndex": prevLogIndex,
			"PrevLogTerm":  prevLogTerm,
			"Entries":      entries,
			"LeaderCommit": rf.commitIndex,
		}
	}

	rf.mu.Unlock()

	var reply map[string]interface{}
	ok := rf.peers[peerIdx].Call(method, args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		replyTerm := reply["Term"].(int)
		if replyTerm > rf.currentTerm {
			rf.transitionTo(Follower, replyTerm)
			return
		}

		if method == "Raft.AppendEntries" {
			if reply["Success"].(bool) {
				entriesCount := len(args.(map[string]interface{})["Entries"].([]interface{}))
				lastSent := args.(map[string]interface{})["PrevLogIndex"].(int) + entriesCount
				rf.matchIndex[peerIdx] = lastSent
				rf.nextIndex[peerIdx] = lastSent + 1
				rf.updateLeaderCommitIndex()
			} else {
				if ni, ok := reply["NextIndex"].(int); ok {
					rf.nextIndex[peerIdx] = max(1, ni)
				} else {
					rf.nextIndex[peerIdx] = max(1, rf.nextIndex[peerIdx]-1)
				}
			}
		} else if method == "Raft.InstallSnapshot" {
			lastIncluded := args.(map[string]interface{})["LastIncludedIndex"].(int)
			rf.matchIndex[peerIdx] = lastIncluded
			rf.nextIndex[peerIdx] = lastIncluded + 1
		}
	}
}

func (rf *Raft) InstallSnapshot(args map[string]interface{}, reply *map[string]interface{}) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := args["Term"].(int)
	if term < rf.currentTerm {
		*reply = map[string]interface{}{"Term": rf.currentTerm}
		return
	}

	if term > rf.currentTerm {
		rf.transitionTo(Follower, term)
	}
	rf.resetElectionTimer()

	lastIdx  := args["LastIncludedIndex"].(int)
	lastTerm := args["LastIncludedTerm"].(int)
	data     := args["Data"].([]byte)

	// Ignore stale snapshots.
	if lastIdx <= rf.lastIncludedIndex {
		*reply = map[string]interface{}{"Term": rf.currentTerm}
		return
	}

	trim := lastIdx - rf.lastIncludedIndex - 1  //position in rf.log
	if trim < len(rf.log) && trim >= 0 {
		rf.log = rf.log[trim+1:]
	} else {
		rf.log = make([]interface{}, 0)
	}
	rf.lastIncludedIndex = lastIdx
	rf.lastIncludedTerm  = lastTerm
	rf.snapshot          = data

	//pply indices so we never re-apply pre-snapshot
	rf.commitIndex  = max(rf.commitIndex,  lastIdx)
	rf.lastApplied  = max(rf.lastApplied,  lastIdx)
	rf.persist()
	*reply = map[string]interface{}{"Term": rf.currentTerm}

	//service layer installing the snapshot
	go func(snapshot []byte, index, term int) {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotIndex: index,
			SnapshotTerm:  term,
		}
	}(data, lastIdx, lastTerm)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)

}


func (rf *Raft) updateLeaderCommitIndex() {
	n := len(rf.peers)
	match := make([]int, n)
	copy(match, rf.matchIndex)
	match[rf.me] = rf.lastIncludedIndex + len(rf.log)

	sort.Sort(sort.Reverse(sort.IntSlice(match)))
	majorityIndex := match[n/2]

	sliceIdx := majorityIndex - rf.lastIncludedIndex - 1
	if sliceIdx >= 0 && sliceIdx < len(rf.log) {
		term := rf.log[sliceIdx].(map[string]interface{})["Term"].(int)
		if term == rf.currentTerm {
			rf.commitIndex = majorityIndex
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) AppendEntries(args map[string]interface{}, reply *map[string]interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args["Term"].(int) < rf.currentTerm {
		*reply = map[string]interface{}{
			"Term":    rf.currentTerm,
			"Success": false,
		}
		return
	}

	if args["Term"].(int) > rf.currentTerm {
		rf.transitionTo(Follower, args["Term"].(int))
	}

	//leader is alive
	rf.resetElectionTimer()

	prevLogIndex := args["PrevLogIndex"].(int)
	prevLogTerm := args["PrevLogTerm"].(int)

	if prevLogIndex > rf.lastIncludedIndex {
		sliceIdx := prevLogIndex - rf.lastIncludedIndex - 1
		//follower’s log is too short
		if sliceIdx >= len(rf.log) {
			*reply = map[string]interface{}{
				"Term":      rf.currentTerm,
				"Success":   false,
				// first index *after* this follower’s log
				"NextIndex": rf.lastIncludedIndex + len(rf.log) + 1,
			}
			return
		}
		//same length but term conflict
		if rf.log[sliceIdx].(map[string]interface{})["Term"].(int) != prevLogTerm {
			conflictTerm := rf.log[sliceIdx].(map[string]interface{})["Term"].(int)
			conflictIdx := rf.lastIncludedIndex + 1
			for i := 0; i < sliceIdx; i++ {
				if rf.log[i].(map[string]interface{})["Term"].(int) == conflictTerm {
					conflictIdx = rf.lastIncludedIndex + i + 1
					break
				}
			}
			*reply = map[string]interface{}{
				"Term":      rf.currentTerm,
				"Success":   false,
				"NextIndex": conflictIdx,
			}
			return
		}

	}
	entries := args["Entries"].([]interface{})
	startIndex := prevLogIndex + 1
	for i, entry := range entries {
		logIdx := startIndex - rf.lastIncludedIndex - 1 + i
		if logIdx >= len(rf.log) {
			rf.log = append(rf.log, entries[i:]...)
			break
		}
		if rf.log[logIdx].(map[string]interface{})["Term"].(int) != entry.(map[string]interface{})["Term"].(int) {
			rf.log = rf.log[:logIdx]
			rf.log = append(rf.log, entries[i:]...)
			break
		}
	}

	leaderCommit := args["LeaderCommit"].(int)
	lastNewEntry := prevLogIndex + len(entries)
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, lastNewEntry)
		rf.applyCond.Broadcast()
	}

	rf.persist()

	*reply = map[string]interface{}{
		"Term":    rf.currentTerm,
		"Success": true,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) applyEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		base := rf.lastIncludedIndex + 1
		var msgs []raftapi.ApplyMsg
		for i := start; i <= end; i++ {
			logIdx := i - base
			if logIdx >= 0 && logIdx < len(rf.log) {
				msgs = append(msgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[logIdx].(map[string]interface{})["Command"],
					CommandIndex: i,
				})
			}
			rf.lastApplied = i
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}