package pipeline

import (
	"context"

	"github.com/vingarcia/go-threads"
	"github.com/vingarcia/go-threads/fanouter"
)

type Stage struct {
	name  string
	tasks []threads.Task
	fanin func(results []interface{}) (interface{}, error)

	numWorkersPerTask threads.Num
}

func (s Stage) NumWorkersPerTask() int {
	return int(s.numWorkersPerTask)
}

func (s Stage) Name() string {
	return s.name
}

func (s Stage) Task() threads.Task {
	return s.tasks[0]
}

type FanoutStage struct {
	Stage
}

func (f FanoutStage) FaninRule(fanin func(results []interface{}) (interface{}, error)) FanoutStage {
	f.fanin = fanin
	return f
}

func (f FanoutStage) Task() threads.Task {
	fan := fanouter.New(context.TODO(), f.tasks...)
	return func(job interface{}) (interface{}, error) {
		debugPrintf("stage `%s` FANNING-OUT\n", f.name)
		jobs, err := fan.Fanout(job)
		if err != nil {
			return nil, err
		}

		debugPrintf("stage `%s` FANNING-IN\n", f.name)
		return f.fanin(jobs)
	}
}

// NewStage instantiates a new Stage with a single task and `numWorkersPerTask` goroutines
func NewStage(name string, numWorkersPerTask threads.Num, task threads.Task) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return Stage{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             []threads.Task{task},
		fanin:             defaultFanin,
	}
}

// NewFanoutStage instantiates a new Stage with a multiple
// tasks each running on `numWorkersPerTask` goroutines
func NewFanoutStage(name string, numWorkersPerTask threads.Num, tasks ...threads.Task) FanoutStage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return FanoutStage{
		Stage{
			name:              name,
			numWorkersPerTask: numWorkersPerTask,
			tasks:             tasks,
		},
	}
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

func defaultFanin(results []interface{}) (interface{}, error) {
	return results[0], nil
}
