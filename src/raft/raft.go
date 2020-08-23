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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器，持久化状态（lab-2A不要求持久化）
	currentTerm int        // 见过的最大任期
	votedFor    int        // 记录在currentTerm任期投票给谁了
	log         []LogEntry // 操作日志
	lastIncludedIndex int 	// snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm int // snapthost最后1个logEntry的term，没有snaphost则无意义

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// 不用加锁，外层逻辑会锁
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// 为了snaphost多做了2个持久化的metadata
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

//
// example RequestVote RPC handler.
//
// todo: lab-3B快照逻辑的支持
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v] ", rf.me, args.CandidateId,
			args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	// 任期不如我大，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走，进行投票
	}

	// 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的日志必须比我的新
		// 1, 最后一条log，任期大的更新
		// 2，任期相同, 更长的log则更新
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		// 这里坑了好久，一定要严格遵守论文的逻辑，另外log长度一样也是可以给对方投票的
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，那么重置自己的下次投票时间
		}
	}
	rf.persist()
}

// todo: lab-3B快照逻辑的支持
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, rf.log, reply.ConflictIndex)
	}()

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// appendEntries RPC , receiver 2)
	// 如果本地没有前一个日志的话，那么false
	if len(rf.log) < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		return
	}
	// 如果本地有前一个日志的话，那么term必须相同，否则false
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
		for index := 1; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
			if rf.log[index-1].Term == reply.ConflictTerm {
				reply.ConflictIndex = index
				break
			}
		}
		return
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[index-1].Term != logEntry.Term {
				rf.log = rf.log[:index-1]         // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新提交下标
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// todo: lab-3B快照逻辑的支持
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化
			elapses := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = now // 重置下次选举时间

				rf.currentTerm += 1 // 发起新任期
				rf.votedFor = rf.me // 该任期投了自己
				rf.persist()

				// 请求投票req
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
				}
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)
				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteCount := 1   // 收到投票个数（先给自己投1票）
				finishCount := 1 // 收到应答个数
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 得到大多数vote后，立即离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

// lab-2A只做心跳，不考虑log同步
// todo: lab-3B快照逻辑的支持
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			// 并发RPC心跳
			type AppendResult struct {
				peerId int
				resp   *AppendEntriesReply
			}

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]LogEntry, 0)
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerId]-1:]...)

				DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, len(rf.log), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)
				// log相关字段在lab-2A不处理
				go func(id int, args1 *AppendEntriesArgs) {
					// DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer func() {
							DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
								rf.me, rf.currentTerm, id, len(rf.log), rf.nextIndex[id], rf.matchIndex[id], rf.commitIndex)
						}()
						// 如果不是rpc前的leader状态了，那么啥也别做了
						if rf.currentTerm != args1.Term {
							return
						}
						if reply.Term > rf.currentTerm { // 变成follower
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							return
						}
						// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
						// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
						if reply.Success { // 同步日志成功
							rf.nextIndex[id] = args1.PrevLogIndex + len(args1.Entries) + 1
							rf.matchIndex[id] = rf.nextIndex[id] - 1

							// 数字N, 让peer[i]的大多数>=N
							// peer[0]' index=2
							// peer[1]' index=2
							// peer[2]' index=1
							// 1,2,2
							// 更新commitIndex, 就是找中位数
							sortedMatchIndex := make([]int, 0)
							sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
							}
							sort.Ints(sortedMatchIndex)
							newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
							if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
						} else {
							// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
							nextIndexBefore := rf.nextIndex[id] // 仅为打印log

							if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term不同
								conflictTermIndex := -1
								for index := args1.PrevLogIndex; index >= 1; index-- { // 找最后一个conflictTerm
									if rf.log[index-1].Term == reply.ConflictTerm {
										conflictTermIndex = index
										break
									}
								}
								if conflictTermIndex != -1 { // leader也存在冲突term的日志，则从term最后一次出现之后的日志开始尝试同步，因为leader/follower可能在该term的日志有部分相同
									rf.nextIndex[id] = conflictTermIndex + 1
								} else { // leader并没有term的日志，那么把follower日志中该term首次出现的位置作为尝试同步的位置，即截断follower在此term的所有日志
									rf.nextIndex[id] = reply.ConflictIndex
								}
							} else { // follower的prevLogIndex位置没有日志
								rf.nextIndex[id] = reply.ConflictIndex + 1
							}
							DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, id, nextIndexBefore, rf.nextIndex[id])
						}
					}
				}(peerId, &args)
			}
		}()
	}
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		var appliedMsgs = make([]ApplyMsg, 0)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.lastApplied - rf.lastIncludedIndex
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex -1].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex -1].Term,
				})
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}()
		// 锁外提交给应用层
		for _, msg := range appliedMsgs {
			applyCh <- msg
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("RaftNode[%d] Make again", rf.me)

	// election逻辑
	go rf.electionLoop()
	// leader逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	go rf.applyLogLoop(applyCh)

	DPrintf("Raftnode[%d]启动", me)

	return rf
}

// 日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

// 保存snapshot，截断log
func (rf *Raft) SaveSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 快照的元信息
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.log[lastIncludedIndex - rf.lastIncludedIndex - 1].Term

	// 压缩内存日志
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex
	afterLog := make([]LogEntry, len(rf.log) - compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	DPrintf("RafeNode[%d] SaveSnapshot, lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)
}

// 加载snapshot（非线程安全，application层启动时候加载一下）
func (rf *Raft) LoadSnapshotUnsafe() (snapshot []byte, lastIncludedIndex int) {
	snapshot = rf.persister.ReadSnapshot()
	lastIncludedIndex = rf.lastIncludedIndex
	// snapshot部分是已应用到状态机的
	rf.lastApplied = rf.lastIncludedIndex
	DPrintf("RaftNode[%d] LoadSnapshotUnsafe, snapshotSize[%d] lastIncludeIndex[%d]",
		rf.me, len(snapshot), lastIncludedIndex)
	return
}