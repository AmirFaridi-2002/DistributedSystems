package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TIMELIMIT = 10 * time.Second
)

type (
	TaskState int
	Action    int
)

const (
	TS_Ongoing TaskState = iota
	TS_Completed
	TS_None
)

const (
	Map_Action Action = iota
	Reduce_Action
)

type Task struct {
	taskType  TaskType
	filename  string
	state     TaskState
	startTime time.Time
	id        int
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	action      Action
}

func (c *Coordinator) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.action == Map_Action {
		c.handleTimedOutTasks(c.mapTasks)

		for i := range c.mapTasks {
			if c.mapTasks[i].state == TS_None {
				c.mapTasks[i].state = TS_Ongoing
				c.mapTasks[i].startTime = time.Now()

				reply.TType = TT_Map
				reply.FileName = c.mapTasks[i].filename
				reply.Map_ID = c.mapTasks[i].id
				reply.NReduce = c.nReduce
				return nil
			}
		}

		if tasksCompleted(c.mapTasks) {
			c.action = Reduce_Action
		} else {
			reply.TType = TT_Wait
			return nil
		}
	}

	if c.action == Reduce_Action {
		c.handleTimedOutTasks(c.reduceTasks)

		for i := range c.reduceTasks {
			if c.reduceTasks[i].state == TS_None {
				c.reduceTasks[i].state = TS_Ongoing
				c.reduceTasks[i].startTime = time.Now()

				reply.TType = TT_Reduce
				reply.Reduce_ID = c.reduceTasks[i].id
				reply.NMap = c.nMap
				return nil
			}
		}

		if tasksCompleted(c.reduceTasks) {
			reply.TType = TT_Exit
		} else {
			reply.TType = TT_Wait
		}
		return nil
	}

	reply.TType = TT_Exit
	return nil
}

// For a worker to report the completion of a task
func (c *Coordinator) RepTask(args *ReportCompleted, reply *AckCompleted) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == TT_Map {
		c.mapTasks[args.Task_ID].state = TS_Completed
	} else if args.TaskType == TT_Reduce {
		c.reduceTasks[args.Task_ID].state = TS_Completed
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.action == Map_Action {
		return false // Map jobs are not finished yet.
	}
	return tasksCompleted(c.reduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		action:      Map_Action,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			taskType: TT_Map,
			filename: file,
			state:    TS_None,
			id:       i,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			taskType: TT_Reduce,
			state:    TS_None,
			id:       i,
		}
	}

	c.server()
	return &c
}

//
//-----------------------------------Helper Functions-----------------------------------
//

func (c *Coordinator) handleTimedOutTasks(tasks []Task) {
	for i := range tasks {
		if tasks[i].state == TS_Ongoing && time.Since(tasks[i].startTime) > TIMELIMIT {
			tasks[i].state = TS_None
		}
	}
}

func tasksCompleted(tasks []Task) bool {
	for i := range tasks {
		if tasks[i].state != TS_Completed {
			return false
		}
	}
	return true
}
