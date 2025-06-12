package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type getReq struct {
	key  string
	resp chan rpc.GetReply
}
type putReq struct {
	key     string
	value   string
	version rpc.Tversion
	resp    chan rpc.PutReply
}

type KVServer struct {
	mu       sync.Mutex
	reqCh    chan interface{}
	values   map[string]string
	versions map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		reqCh:    make(chan interface{}),
		values:   make(map[string]string),
		versions: make(map[string]rpc.Tversion),
	}
	go kv.loop()
	return kv
}

type request interface {
	exec(*KVServer)
}

func (g getReq) exec(kv *KVServer) {
	kv.mu.Lock()
	DPrintf("Get %q", g.key)
	v, ok := kv.values[g.key]
	ver := kv.versions[g.key]
	kv.mu.Unlock()
	switch ok {
	case true:
		g.resp <- rpc.GetReply{Value: v, Version: ver, Err: rpc.OK}
	default:
		g.resp <- rpc.GetReply{Err: rpc.ErrNoKey}
	}
}

func (p putReq) exec(kv *KVServer) {
	kv.mu.Lock()
	DPrintf("Put %q@v%d=%q", p.key, p.version, p.value)
	curr, exists := kv.versions[p.key]
	var err rpc.Err
	switch {
	case !exists && p.version == 0:
		kv.values[p.key], kv.versions[p.key], err = p.value, 1, rpc.OK
	case !exists:
		err = rpc.ErrNoKey
	case curr == p.version:
		kv.values[p.key], kv.versions[p.key], err = p.value, curr+1, rpc.OK
	default:
		err = rpc.ErrVersion
	}
	kv.mu.Unlock()
	p.resp <- rpc.PutReply{Err: err}
}

func (kv *KVServer) loop() {
	for req := range kv.reqCh {
		req.(request).exec(kv)
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	ch := make(chan rpc.GetReply, 1)
	kv.reqCh <- getReq{key: args.Key, resp: ch}
	*reply = <-ch
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	ch := make(chan rpc.PutReply, 1)
	kv.reqCh <- putReq{
		key:     args.Key,
		value:   args.Value,
		version: args.Version,
		resp:    ch,
	}
	*reply = <-ch
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
