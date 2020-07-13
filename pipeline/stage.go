package pipeline

import (
	"context"

	"github.com/vingarcia/go-threads"
	"github.com/vingarcia/go-threads/fanouter"
)

type Stage struct {
	Name    string
	Task    threads.Task
	Threads int
}

func (s Stage) NumThreads() int {
	return s.Threads
}

func (s Stage) GetName() string {
	return s.Name
}

func (s Stage) BuildWorker(ctx context.Context) threads.Task {
	return s.Task
}

type FanoutStage struct {
	Name           string
	Tasks          []threads.Task
	ThreadsPerTask int
	FaninRule      func(results []interface{}) (interface{}, error)
}

func (f FanoutStage) NumThreads() int {
	return f.ThreadsPerTask
}

func (f FanoutStage) GetName() string {
	return f.Name
}

func (f FanoutStage) BuildWorker(ctx context.Context) threads.Task {
	if f.ThreadsPerTask < 1 {
		f.ThreadsPerTask = 1
	}
	if f.FaninRule == nil {
		f.FaninRule = defaultFanin
	}

	fan := fanouter.New(ctx, f.Tasks...)
	return func(job interface{}) (interface{}, error) {
		debugPrintf("stage `%s` FANNING-OUT\n", f.Name)
		jobs, err := fan.Fanout(job)
		if err != nil {
			return nil, err
		}

		debugPrintf("stage `%s` FANNING-IN\n", f.Name)
		return f.FaninRule(jobs)
	}
}

func defaultFanin(results []interface{}) (interface{}, error) {
	return results[0], nil
}

func buildStageWorker(
	ctx context.Context,
	stageIdx int,
	stageName string,
	inputCh chan interface{},
	outputCh chan interface{},
	task threads.Task,
) func() error {
	return func() error {
		var job interface{}
		for {
			if inputCh != nil {
				debugPrintf("stage `%s` (n%d) reading from %v\n", stageName, stageIdx, inputCh)
				job = <-inputCh
			}

			resp, err := task(job)
			if err != nil {
				return err
			}

			if outputCh != nil {
				debugPrintf("stage `%s` (n%d) writing to %v\n", stageName, stageIdx, outputCh)
				outputCh <- resp
			}
		}
	}
}
