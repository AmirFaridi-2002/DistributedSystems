package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		args := ReqTaskArgs{}
		reply := ReqTaskReply{}
		ok := call("Coordinator.ReqTask", &args, &reply)
		if !ok {
			return
		}

		switch reply.TType {
		case TT_Map:
			HandleMap(reply, mapf)
		case TT_Reduce:
			HandleReduce(reply, reducef)
		case TT_Wait:
			time.Sleep(time.Second)
		case TT_Exit:
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func HandleMap(reply ReqTaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	mapID := reply.Map_ID
	nReduce := reply.NReduce

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("(Worker) Couldn't open %v: %v", filename, err)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("(Worker) Couldn't read %v: %v", filename, err)
	}
	file.Close()

	intermediate := mapf(filename, string(content))
	partitions := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		partitions[i] = append(partitions[i], kv)
	}

	for idx, kvs := range partitions {
		tmpfile, err := os.CreateTemp("", "mr-tmp")
		if err != nil {
			log.Fatalf("(Worker) Couldn't create temp file: %v", err)
		}

		encoder := json.NewEncoder(tmpfile)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("(Worker) Couldn't encode pairs: %v", err)
			}
		}
		tmpfile.Close()
		newname := fmt.Sprintf("mr-%d-%d", mapID, idx)
		err = os.Rename(tmpfile.Name(), newname)
		if err != nil {
			log.Fatalf("(Worker) Couldn't rename temp file to %s: %v", newname, err)
		}
	}

	reportCompleted := ReportCompleted{
		TaskType: TT_Map,
		Task_ID:  mapID,
	}
	ackCompleted := AckCompleted{}
	call("Coordinator.RepTask", &reportCompleted, &ackCompleted)
}

func HandleReduce(reply ReqTaskReply, reducef func(string, []string) string) {
	reduceID := reply.Reduce_ID
	nMap := reply.NMap

	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	outputname := fmt.Sprintf("mr-out-%d", reduceID)
	outputfile, err := os.Create(outputname)
	if err != nil {
		log.Fatalf("(Worker) Couldn't create output file %s: %v", outputname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outputfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outputfile.Close()

	reportCompleted := ReportCompleted{
		TaskType: TT_Reduce,
		Task_ID:  reduceID,
	}
	ackCompleted := AckCompleted{}
	call("Coordinator.RepTask", &reportCompleted, &ackCompleted)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
