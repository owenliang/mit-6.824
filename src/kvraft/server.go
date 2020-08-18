package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_TYPE_PUT = "Put"
	OP_TYPE_PUT_APPEND = "PutAppend"
	OP_TYPE_GET = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index int // 写入raft log时的index
	Term int // 写入raft log时的term
	Type string // PutAppend, Get
	Key string
	Value string
	ReqId int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	mu sync.Mutex
	cond *sync.Cond
	end bool	// raft log对应index提交后设置为true
	index int 	// raft真正提交的index
	op *Op
	// Get操作
	keyExist bool
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string	// kv存储

	reqMap map[int64]*OpContext	// 请求上下文
}

func newOpContext(op *Op) (opCtx *OpContext){
	opCtx = &OpContext{
		op: op,
	}
	opCtx.cond = sync.NewCond(&opCtx.mu)
	return
}

func (opCtx *OpContext) waitForCommit() {
	// 等待日志提交
	opCtx.mu.Lock()
	for {
		if opCtx.end {
			break
		} else {
			opCtx.cond.Wait()
		}
	}
	opCtx.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK

	op := &Op{
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		ReqId: args.ReqId,
	}

	opCtx := newOpContext(op)

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 重试的请求, 等待先前的日志提交即可
		if curOpCtx, exist := kv.reqMap[op.ReqId]; exist {
			opCtx = curOpCtx
		} else {
			kv.reqMap[op.ReqId] = opCtx

			var isLeader bool
			op.Index, op.Term, isLeader = kv.rf.Start(op)
			if !isLeader {
				delete(kv.reqMap, op.ReqId)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}()

	// 等待日志提交
	opCtx.waitForCommit()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLoop() {
	for {
		select {
		case msg := <- kv.applyCh:
			DPrintf("%v", msg)
			cmd := msg.Command
			index := msg.CommandIndex
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				// 写入到KV存储里
				op := cmd.(Op)
				if op.Type == OP_TYPE_PUT {
					kv.kvStore[op.Key] = op.Value
				} else if op.Type == OP_TYPE_PUT_APPEND {
					if val, exist := kv.kvStore[op.Key]; exist {
						kv.kvStore[op.Key] = val + op.Value
					} else {
						kv.kvStore[op.Key] = op.Value
					}
				} else {	// GET

				}

				// RPC存在，全部唤醒
				if opCtx, exist := kv.reqMap[op.ReqId]; exist {
					delete(kv.reqMap, op.ReqId)

					opCtx.mu.Lock()
					opCtx.end = true
					opCtx.index = index
					if op.Type == OP_TYPE_GET {
						opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
					}
					opCtx.cond.Broadcast()
					opCtx.mu.Unlock()
				}
			}()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int64]*OpContext)

	return kv
}
