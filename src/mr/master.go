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

type Master struct {
	// Your definitions here.
	mu              sync.Mutex
	MapperFinished  bool           // mapper是否全部完成
	ReducerFinished bool           // reducer是否全部完成
	Mappers         []*MapperTask  // mapper任务
	Reducers        []*ReducerTask // reducer任务
}

type MapperTask struct {
	Index        int       // 任务编号
	Assigned     bool      // 是否分配
	AssignedTime time.Time // 分配时间
	IsFinished   bool      // 是否完成

	InputFile    string // 输入文件
	ReducerCount int    // 有多少路reducer

	timeoutTimer *time.Timer // 任务超时
}

type ReducerTask struct {
	Index        int       // 任务编号
	Assigned     bool      // 是否分配
	AssignedTime time.Time // 分配时间
	IsFinished   bool      // 是否完成

	MapperCount int //	有多少路mapper

	timeoutTimer *time.Timer // 任务超时
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) startMapper(mapper *MapperTask) {
	mapper.Assigned = true
	mapper.AssignedTime = time.Now()
	mapper.timeoutTimer = time.AfterFunc(10*time.Second, func(index int) func() {
		return func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if !m.Mappers[index].IsFinished {
				m.Mappers[index].Assigned = false
			}
		}
	}(mapper.Index))
}

func (m *Master) startReducer(reducer *ReducerTask) {
	reducer.Assigned = true
	reducer.AssignedTime = time.Now()
	reducer.timeoutTimer = time.AfterFunc(10*time.Second, func(index int) func() {
		return func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if !m.Reducers[index].IsFinished {
				m.Reducers[index].Assigned = false
			}
		}
	}(reducer.Index))
}

func (m *Master) finishMapper(mapper *MapperTask) {
	mapper.IsFinished = true
	mapper.timeoutTimer.Stop()
}

func (m *Master) finishReducer(reducer *ReducerTask) {
	reducer.IsFinished = true
	reducer.timeoutTimer.Stop()
}

func (m *Master) FetchTask(request *FetchTaskRequest, response *FetchTaskResponse) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.MapperFinished {
		for _, mapper := range m.Mappers {
			if mapper.Assigned || mapper.IsFinished {
				continue
			}
			m.startMapper(mapper)
			task := *mapper // 副本
			response.MapperTask = &task
			return
		}
		return // 所有mapper任务都分配出去了，那么暂时没有工作了
	}
	if !m.ReducerFinished {
		for _, reducer := range m.Reducers {
			if reducer.Assigned || reducer.IsFinished {
				continue
			}
			m.startReducer(reducer)
			task := *reducer
			response.ReducerTask = &task
			return
		}
		return // 所有reducer任务都分配出去了，那么暂时没有工作了
	}
	response.AllFinished = true
	return
}

func (m *Master) UpdateTask(request *UpdateTaskRequest, response *UpdateTaskResponse) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if request.Mapper != nil {
		MapperFinished := true
		for _, mapper := range m.Mappers {
			if mapper.Index == request.Mapper.Index && mapper.Assigned && !mapper.IsFinished {
				m.finishMapper(mapper)
			}
			MapperFinished = MapperFinished && mapper.IsFinished
		}
		m.MapperFinished = MapperFinished
	}
	if request.Reducer != nil {
		ReducerFinished := true
		for _, reducer := range m.Reducers {
			if reducer.Index == request.Reducer.Index && reducer.Assigned && !reducer.IsFinished {
				m.finishReducer(reducer)
			}
			ReducerFinished = ReducerFinished && reducer.IsFinished
		}
		m.ReducerFinished = ReducerFinished
	}
	return
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.MapperFinished && m.ReducerFinished

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// 1, Mapper任务列表
	m.Mappers = make([]*MapperTask, 0)
	for i, file := range files {
		mapper := &MapperTask{
			Index:        i,
			Assigned:     false,
			AssignedTime: time.Now(),
			IsFinished:   false,
			InputFile:    file,
			ReducerCount: nReduce,
		}
		m.Mappers = append(m.Mappers, mapper)
	}

	// 2，Reducer任务列表
	m.Reducers = make([]*ReducerTask, 0)
	for i := 0; i < nReduce; i++ {
		reducer := &ReducerTask{
			Index:        i,
			Assigned:     false,
			AssignedTime: time.Now(),
			IsFinished:   false,
			MapperCount:  len(files),
		}
		m.Reducers = append(m.Reducers, reducer)
	}

	m.server()
	return &m
}
