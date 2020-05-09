package pipeline

import (
	"context"
	"fmt"

	"github.com/vingarcia/go-pipeline/fanouter"
	"golang.org/x/sync/errgroup"
)

// Pipeline organizes several goroutines to process a
// number of user defined tasks in the form of a pipeline.
//
// Each stage of this pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
type Pipeline struct {
	stages []Stage

	started bool
	Debug   bool
}

// New is used to instantiate the Stages and Tasks
// of a new Pipeline.
//
// Each stage of this pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
func New(stages ...Stage) Pipeline {
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

		for j := 0; j < int(stage.NumWorkersPerTask()); j++ {
			switch stage := stage.(type) {
			case StageT:
				g.Go(stage.stageWorker(ctx, idx, p, inputCh, outputCh, stage.tasks[0]))
			case FanoutStage:
				fan := fanouter.New(ctx, stage.tasks...)
				g.Go(stage.stageWorker(ctx, idx, p, inputCh, outputCh, func(job interface{}) (interface{}, error) {
					return fan.Fanout(job)
				}))
			}
		}

		// Create the input channel of the next stage:
		nextInputCh = outputCh
	}

	return g.Wait()
}

func (p Pipeline) debugPrintf(format string, args ...interface{}) {
	if p.Debug {
		fmt.Printf(format, args...)
	}
}
