package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	TT_Map TaskType = iota
	TT_Reduce
	TT_Wait
	TT_Exit
)

type ReqTaskArgs struct{}

type ReqTaskReply struct {
	TType     TaskType
	FileName  string
	NMap      int
	NReduce   int
	Map_ID    int
	Reduce_ID int
}

type ReportCompleted struct {
	TaskType TaskType
	Task_ID  int
}

type AckCompleted struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
