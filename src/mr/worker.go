package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Mapper
func Mapper(mapf func(string, string) []KeyValue, reply Reply) {
	filename := reply.FileName
	mapID := reply.MapID

	nReduce := reply.NReduce
	//fmt.Printf("file = %s\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	enc := make([]*json.Encoder, nReduce)
	tmpFiles := make([]string, nReduce)
	for reduceID := 0; reduceID < nReduce; reduceID++ {
		intermeditafile := fmt.Sprintf("Map-tmp-%v-", mapID)
		//fmt.Println(intermeditafile)
		tmpfile, err := ioutil.TempFile("./", intermeditafile)
		//tmpfile, err := os.Create(intermeditafile)

		if err != nil {
			log.Fatalf("cannot create %v", intermeditafile)
		}
		enc[reduceID] = json.NewEncoder(tmpfile)
		tmpFiles[reduceID] = tmpfile.Name()
	}

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		enc[reduceID].Encode(&kv)
	}

	ackArgs := Args{
		MapID:       reply.MapID,
		ReduceID:    reply.ReduceID,
		Task:        reply.Task,
		TmpFileName: tmpFiles,
	}
	//fmt.Printf("Complete %v map task\n", mapID)

	ackReply := Reply{}
	call("Coordinator.Complete", &ackArgs, &ackReply)
	//fmt.Println("Ack done")

}

// Reducer
func Reducer(reducef func(string, []string) string, reply Reply) {
	//fmt.Println("Reduce task", reply.ReduceID)
	nMap := reply.MapID
	reduceID := reply.ReduceID
	dec := make([]*json.Decoder, nMap)
	intermeditafiles := make([]string, nMap)
	// Create respective decoder for intermidate files
	for i := 0; i < nMap; i++ {
		intermeditafiles[i] = fmt.Sprintf("mr-%v-%v", i, reduceID)
		tmpfile, err := os.Open(intermeditafiles[i])

		if err != nil {
			log.Fatalf("cannot open %v", intermeditafiles[i])
		}
		dec[i] = json.NewDecoder(tmpfile)
	}

	intermediate := []KeyValue{}

	// Read Key Value pair back
	for i := 0; i < nMap; i++ {
		for {
			var kv KeyValue
			if err := dec[i].Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceID)
	ofile, _ := ioutil.TempFile("./", oname)
	defer os.Remove(ofile.Name())

	//fmt.Println(oname)
	// Reduce task
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
	for i := 0; i < nMap; i++ {
		//fmt.Println("remove", intermeditafiles[i])
		os.Remove(intermeditafiles[i])
	}

	OutputFile := make([]string, 1)
	OutputFile[0] = ofile.Name()

	ackArgs := Args{
		MapID:       reply.MapID,
		ReduceID:    reply.ReduceID,
		Task:        reply.Task,
		TmpFileName: OutputFile,
	}

	ackReply := Reply{}
	call("Coordinator.Complete", &ackArgs, &ackReply)

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		args := Args{}
		reply := Reply{}
		//fmt.Println("call RPC")
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {
			switch reply.Task {
			case 0:
				Mapper(mapf, reply)
			case 1:
				Reducer(reducef, reply)
			case 2:
				return
			}
		} else {
			//fmt.Printf("call failed\n")
			time.Sleep(1 * time.Second)
			continue
		}

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	//fmt.Println(err)
	return false
}
