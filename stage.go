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
type Stage struct {
	name  string
	tasks []async.Task

	outputCh chan interface{}

	numWorkersPerTask async.TaskForce
}

// NewStage instantiates a new Stage with a single task and `numWorkersPerTask` goroutines
func NewStage(name string, numWorkersPerTask async.TaskForce, task async.Task) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return Stage{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             []async.Task{task},
		outputCh:          make(chan interface{}, numWorkersPerTask),
	}
}

// NewFanoutStage instantiates a new Stage with a multiple
// tasks each running on `numWorkersPerTask` goroutines
func NewFanoutStage(name string, numWorkersPerTask async.TaskForce, tasks ...async.Task) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return Stage{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             tasks,
	}
}

func stageWorker(
	ctx context.Context,
	stageIdx int,
	pipe Pipeline,
	inputCh chan interface{},
	task async.Task,
) func() error {
	stage := &pipe.stages[stageIdx]
	return func() error {
		var job interface{}
		for {
			pipe.debugPrintf("stage `%s` (n%d) reading from %v\n", stage.name, stageIdx, inputCh)

			if stageIdx > 0 {
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

			pipe.debugPrintf("stage `%s` (n%d) writing to %v\n", stage.name, stageIdx, stage.outputCh)
			stage.outputCh <- resp
		}
	}
}
