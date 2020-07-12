package pipeline

import (
	"context"

	thread "github.com/vingarcia/go-pipeline"
	"github.com/vingarcia/go-pipeline/fanouter"
)

type StageT struct {
	name  string
	tasks []thread.Task
	fanin func(results []interface{}) (interface{}, error)

	numWorkersPerTask thread.TaskForce
}

func (s StageT) NumWorkersPerTask() int {
	return int(s.numWorkersPerTask)
}

func (s StageT) Name() string {
	return s.name
}

func (s StageT) Task() thread.Task {
	return s.tasks[0]
}

type FanoutStage struct {
	StageT
}

func (f FanoutStage) FaninRule(fanin func(results []interface{}) (interface{}, error)) FanoutStage {
	f.fanin = fanin
	return f
}

func (f FanoutStage) Task() thread.Task {
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
func NewStage(name string, numWorkersPerTask thread.TaskForce, task thread.Task) StageT {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return StageT{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             []thread.Task{task},
		fanin:             defaultFanin,
	}
}

// NewFanoutStage instantiates a new Stage with a multiple
// tasks each running on `numWorkersPerTask` goroutines
func NewFanoutStage(name string, numWorkersPerTask thread.TaskForce, tasks ...thread.Task) FanoutStage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return FanoutStage{
		StageT{
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
	task thread.Task,
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
