package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// worker下载任务
type FetchTaskRequest struct {

}
type FetchTaskResponse struct {
	AllFinished bool // 任务全部完成
	MapperTask *MapperTask
	ReducerTask *ReducerTask
}

// worker上报进度
type UpdateTaskRequest struct {
	Mapper *MapperTask
	Reducer *ReducerTask
}
type UpdateTaskResponse struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
