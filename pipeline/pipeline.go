package pipeline

import (
	"context"
	"fmt"

	thread "github.com/vingarcia/go-pipeline"
	"golang.org/x/sync/errgroup"
)

var Debug = false

// Pipeline organizes several goroutines to process a
// number of user defined tasks in the form of a pipeline.
//
// Each stage of this pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
type Pipeline struct {
	stages []thread.Stage

	started bool
	Debug   bool
}

// New is used to instantiate the Stages and Tasks
// of a new Pipeline.
//
// Each stage of this pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
func New(stages ...thread.Stage) Pipeline {
	return Pipeline{
		stages: stages,
	}
}

// Start the pipeline processing.
//
// This function blocks and only stops when an error occur
// withing the pipeline.
func (p Pipeline) Start() error {
	return p.StartWithContext(context.TODO())
}

// StartWithContext starts the pipeline inside a context
//
// This function blocks and only stops when an error occur
// withing the pipeline or the context is canceled
func (p Pipeline) StartWithContext(ctx context.Context) error {
	var nextInputCh chan interface{}
	var g errgroup.Group

	for idx := range p.stages {
		stage := p.stages[idx]

		// nil on the first iteration:
		inputCh := nextInputCh
		outputCh := make(chan interface{})
		// outputCh is nil on the last iteration:
		if idx == len(p.stages)-1 {
			outputCh = nil
		}

		for j := 0; j < int(stage.NumWorkersPerTask()); j++ {
			g.Go(buildStageWorker(ctx, idx, stage.Name(), inputCh, outputCh, stage.Task()))
		}

		// Create the input channel of the next stage:
		nextInputCh = outputCh
	}

	return g.Wait()
}

func debugPrintf(format string, args ...interface{}) {
	if Debug {
		fmt.Printf(format, args...)
	}
}
