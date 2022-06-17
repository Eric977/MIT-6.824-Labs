package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status struct {
	worker    int
	status    int // 0 for idle, 1 for running, 2 for complete
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	stage          int
	FileNames      []string
	MapStatus      []Status
	ReduceStatus   []Status
	MapComplete    int
	ReduceComplete int
	MapID          int // Next Map task
	reduceID       int // Next Reduce task
	LastComplete   int
	nReduce        int // # of Reduce task
	nMap           int // # of Map task
}

func (c *Coordinator) SetMapReply(reply *Reply, mapID int) {
	c.MapStatus[mapID].startTime = time.Now()
	reply.Task = 0
	reply.FileName = c.FileNames[mapID]
	reply.MapID = mapID
	reply.NReduce = c.nReduce

}

func (c *Coordinator) SetReduceReply(reply *Reply, reduceID int) {
	c.ReduceStatus[reduceID].startTime = time.Now()
	reply.Task = 1
	reply.MapID = c.nMap
	reply.ReduceID = reduceID

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignMapTask(args *Args, reply *Reply) int {
	for {
		if c.stage == 1 {
			return 1
		}
		if c.MapID < c.nMap {
			c.SetMapReply(reply, c.MapID)
			c.MapID++
			return 1
		} else {
			for i := 0; i < c.nMap; i++ {
				if c.MapStatus[i].status == 2 {
					//c.LastComplete++
					continue
				} else {
					elapsed := time.Since(c.MapStatus[i].startTime)
					//fmt.Printf("%v elapsed %v second\n", i, elapsed.Seconds())
					if elapsed.Seconds() > 10 {
						//fmt.Printf("Reissue %v map Task\n", i)
						c.SetMapReply(reply, i)
						return 1
					}
				}
			}
			return 0
		}
	}

}

func (c *Coordinator) AssignReduceTask(args *Args, reply *Reply) int {
	for {
		if c.stage == 2 {
			return 1
		}
		if c.reduceID < c.nReduce {
			c.SetReduceReply(reply, c.reduceID)
			c.reduceID++
			return 1
		} else {
			for i := 0; i < c.nReduce; i++ {
				if c.ReduceStatus[i].status == 2 {
					continue
				} else {
					elapsed := time.Since(c.ReduceStatus[i].startTime)
					//fmt.Printf("%v elapsed %v second\n", i, elapsed.Seconds())
					if elapsed.Seconds() > 10 {
						//fmt.Printf("Reissue %v reduce Task\n", i)
						c.SetReduceReply(reply, i)
						return 1
					}
				}
			}
			return 0
		}

	}
}
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	ok := 1
	c.mu.Lock()
	switch c.stage {
	case 0:
		ok = c.AssignMapTask(args, reply)
	case 1:
		ok = c.AssignReduceTask(args, reply)
	case 2:
		reply.Task = 2
	}
	if ok == 1 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	return errors.New("Assign no task\n")
}

func (c *Coordinator) Complete(args *Args, reply *Reply) error {
	c.mu.Lock()
	MapID := args.MapID
	ReduceID := args.ReduceID

	//fmt.Printf("Get task %v ack\n", MapID)

	switch args.Task {
	case 0:
		if c.MapStatus[MapID].status != 2 {
			for i := 0; i < c.nReduce; i++ {
				newpath := fmt.Sprintf("mr-%v-%v", MapID, i)
				os.Rename(args.TmpFileName[i], newpath)
			}
			c.MapStatus[MapID].status = 2
			c.MapComplete++
			//fmt.Printf("Comple %v MapTask\n", MapID)
		}
		if c.MapComplete >= c.nMap {
			c.stage = 1
		}
	case 1:
		if c.ReduceStatus[ReduceID].status != 2 {
			newpath := fmt.Sprintf("mr-out-%v", args.ReduceID)
			os.Rename(args.TmpFileName[0], newpath)
			c.ReduceComplete++
			c.ReduceStatus[ReduceID].status = 2

		}
		if c.ReduceComplete == c.nReduce {
			c.stage = 2
		}
	}
	c.mu.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	ret := c.stage == 2
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames:    files,
		nMap:         len(files),
		nReduce:      nReduce,
		reduceID:     0,
		MapID:        0,
		LastComplete: 0,
	}

	// Your code here.
	c.MapStatus = make([]Status, c.nMap)
	c.ReduceStatus = make([]Status, c.nReduce)

	c.server()
	return &c
}
