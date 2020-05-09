package pipeline

import (
	"context"

	"github.com/vingarcia/go-pipeline/async"
)

// Stage describes an stage of the Pipeline
//
// Each stage of the pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
type Stage interface {
	stageWorker(
		ctx context.Context,
		stageIdx int,
		pipe Pipeline,
		inputCh chan interface{},
		outputCh chan interface{},
		task async.Task,
	) func() error

	NumWorkersPerTask() int
}

type StageT struct {
	name  string
	tasks []async.Task
	fanin func(results []interface{}) (interface{}, error)

	numWorkersPerTask async.TaskForce
}

func (s StageT) NumWorkersPerTask() int {
	return int(s.numWorkersPerTask)
}

type FanoutStage struct {
	StageT
}

func (f FanoutStage) FaninRule(fanin func(results []interface{}) (interface{}, error)) FanoutStage {
	f.fanin = fanin
	return f
}

// NewStage instantiates a new Stage with a single task and `numWorkersPerTask` goroutines
func NewStage(name string, numWorkersPerTask async.TaskForce, task async.Task) StageT {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return StageT{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             []async.Task{task},
		fanin:             defaultFanin,
	}
}

// NewFanoutStage instantiates a new Stage with a multiple
// tasks each running on `numWorkersPerTask` goroutines
func NewFanoutStage(name string, numWorkersPerTask async.TaskForce, tasks ...async.Task) FanoutStage {
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

func (stage StageT) stageWorker(
	ctx context.Context,
	stageIdx int,
	pipe Pipeline,
	inputCh chan interface{},
	outputCh chan interface{},
	task async.Task,
) func() error {
	return func() error {
		var job interface{}
		for {
			if stageIdx > 0 {
				pipe.debugPrintf("stage `%s` (n%d) reading from %v\n", stage.name, stageIdx, inputCh)
				job = <-inputCh
			}

			resp, err := task(job)
			if err != nil {
				return err
			}

			// The last stage doesn't write anything
			if stageIdx == len(pipe.stages)-1 {
				continue
			}

			pipe.debugPrintf("stage `%s` (n%d) writing to %v\n", stage.name, stageIdx, outputCh)
			outputCh <- resp
		}
	}
}

func defaultFanin(results []interface{}) (interface{}, error) {
	return results[0], nil
}
