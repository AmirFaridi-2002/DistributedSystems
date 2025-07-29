package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	client   kvtest.IKVClerk
	name    string
	token   string
	backoff time.Duration
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, name string) *Lock {
	return &Lock{
		client:  ck,
		name:    name,
		token:   kvtest.RandValue(8),
		backoff: 10 * time.Millisecond,
	}
}

func (l *Lock) Acquire() {
	attempt := func() bool {
		val, ver, err := l.client.Get(l.name)
		switch {
		case err == rpc.ErrNoKey || (err == rpc.OK && val == ""):
			expVer := ver
			if err == rpc.ErrNoKey {
				expVer = 0
			}
			switch putErr := l.client.Put(l.name, l.token, expVer); {
			case putErr == rpc.OK:
				return true
			case putErr == rpc.ErrMaybe && l.confirmOwnership():
				return true
			default:
				return false
			}
		default:
			return false
		}
	}
	if attempt() {
		return
	}
	ticker := time.NewTicker(l.backoff)
	defer ticker.Stop()
	for range ticker.C {
		if attempt() {
			return
		}
	}
}

func (l *Lock) Release() {
	attempt := func() bool {
		val, ver, err := l.client.Get(l.name)
		switch {
		case err != rpc.OK || val != l.token:
			return true
		default:
			switch putErr := l.client.Put(l.name, "", ver); {
			case putErr == rpc.OK:
				return true
			case putErr == rpc.ErrMaybe && l.confirmRelease():
				return true
			default:
				return false
			}
		}
	}
	if attempt() {
		return
	}
	ticker := time.NewTicker(l.backoff)
	defer ticker.Stop()
	for range ticker.C {
		if attempt() {
			return
		}
	}
}

func (l *Lock) confirmOwnership() bool {
	curr, _, err := l.client.Get(l.name)
	return err == rpc.OK && curr == l.token
}

func (l *Lock) confirmRelease() bool {
	curr, _, err := l.client.Get(l.name)
	return err == rpc.OK && curr == ""
}
