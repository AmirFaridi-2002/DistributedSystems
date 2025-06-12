package kvsrv

import (
	"reflect"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	type request struct {
		key string
	}
	type result struct {
		value   string
		version rpc.Tversion
		err     rpc.Err
	}
	req := request{key}
	res := result{}
	{
		args := &rpc.GetArgs{Key: req.key}
		reply := &rpc.GetReply{}
		rpcCallWithRetry(ck.clnt, ck.server, "KVServer.Get", args, reply)
		res.value = reply.Value
		res.version = reply.Version
		res.err = reply.Err
	}
	return res.value, res.version, res.err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	type request struct {
		key     string
		value   string
		version rpc.Tversion
	}
	type result struct {
		err rpc.Err
	}
	req := request{key, value, version}
	res := result{}
	{
		args := &rpc.PutArgs{
			Key:     req.key,
			Value:   req.value,
			Version: req.version,
		}
		reply := &rpc.PutReply{}
		rpcCallWithRetry(ck.clnt, ck.server, "KVServer.Put", args, reply)
		res.err = reply.Err
	}
	return res.err
}

func rpcCallWithRetry(c *tester.Clnt, srv, m string, args, reply any) {
	var errPtr *rpc.Err
	if rv := reflect.ValueOf(reply); rv.Kind() == reflect.Ptr && !rv.IsNil() {
		elem := rv.Elem()
		if elem.Kind() == reflect.Struct {
			if f := elem.FieldByName("Err"); f.IsValid() && f.Type() == reflect.TypeOf(rpc.Err("")) && f.CanAddr() {
				errPtr = f.Addr().Interface().(*rpc.Err)
			}
		}
	}
	firstAttempt := true
	for {
		ok := c.Call(srv, m, args, reply)
		if ok {
			if !firstAttempt && errPtr != nil {
				if *errPtr == rpc.ErrVersion {
					*errPtr = rpc.ErrMaybe
				}
			}
			return
		}
		firstAttempt = false
		time.Sleep(100 * time.Millisecond)
	}
}
