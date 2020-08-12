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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		resp := CallFetchTask()
		if resp == nil {
			continue
		}

		if resp.AllFinished {
			return 
		}

		if resp.MapperTask != nil {
			// 做mapper的事情
			doMapperTask(resp.MapperTask, mapf)
		}
		if resp.ReducerTask != nil {
			// 做reducer的事情
			doReducerTask(resp.ReducerTask, reducef)
		}
	}
}

func doReducerTask(reducerTask *ReducerTask, reducef func(string, []string) string) {
	kvs := make([]KeyValue, 0)
	for i:=0; i < reducerTask.MapperCount;i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reducerTask.Index)
		fp, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(fp)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(ByKey(kvs)) // 按key排序的k-v列表

	ofileName := fmt.Sprintf("mr-out-%d", reducerTask.Index)
	ofile, err  := os.Create(ofileName + ".tmp")
	if err != nil {
		log.Fatalf("cannot open %v", ofileName + ".tmp")
	}

	// [i,j]
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofileName + ".tmp", ofileName)
	// fmt.Printf("Reducer[%d] InputFiles=[%v]\n", reducerTask.Index, reduceFiles)
	CallUpdateTaskForReducer(reducerTask)
}

func doMapperTask(mapperTask *MapperTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(mapperTask.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", mapperTask.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapperTask.InputFile)
	}
	file.Close()
	kva := mapf(mapperTask.InputFile, string(content))

	// mapper的shuffle
	// 10个reducer
	// 每1个mapper输出10路文件，对key做hash%10
	reducerKvArr := make([][]KeyValue, mapperTask.ReducerCount)

	for _, kv := range kva {
		reducerNum := ihash(kv.Key) % mapperTask.ReducerCount
		reducerKvArr[reducerNum] = append(reducerKvArr[reducerNum], kv)
	}

	for i, kvs := range reducerKvArr {
		sort.Sort(ByKey(kvs))

		filename := fmt.Sprintf("mr-%d-%d", mapperTask.Index, i)
		file, err := os.Create(filename + ".tmp")
		if err != nil {
			log.Fatalf("cannot write %v", filename + ".tmp")
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot jsonencode %v", filename + ".tmp")
			}
		}
		file.Close()
		os.Rename(filename + ".tmp", filename)
	}

	CallUpdateTaskForMapper(mapperTask)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallFetchTask() (ret *FetchTaskResponse) {
	req := FetchTaskRequest{}
	resp := FetchTaskResponse{}
	if call("Master.FetchTask", &req, &resp) {
		ret = &resp
	}
	return
}

func CallUpdateTaskForMapper(mapper *MapperTask) {
	req := UpdateTaskRequest{Mapper: mapper}
	resp := UpdateTaskResponse{}
	call("Master.UpdateTask", &req, &resp)
}

func CallUpdateTaskForReducer(reducer *ReducerTask) {
	req := UpdateTaskRequest{Reducer: reducer}
	resp := UpdateTaskResponse{}
	call("Master.UpdateTask", &req, &resp)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
