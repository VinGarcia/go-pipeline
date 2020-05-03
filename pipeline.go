package pipeline

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type NumWorkers int

// Pipeline ...
type Pipeline struct {
	stages []Stage

	started bool
	Debug   bool
}

// New ...
func New(stages ...Stage) Pipeline {
	return Pipeline{
		stages: stages,
	}
}

// Start ...
func (p Pipeline) Start() error {
	return p.StartWithContext(context.Background())
}

// StartWithContext ...
func (p Pipeline) StartWithContext(ctx context.Context) error {
	p.started = true

	var nextInputCh chan interface{}
	var g errgroup.Group

	// Prepare input & output channels:
	for i := range p.stages {
		stage := &p.stages[i]
		stage.outputCh = make(chan interface{}, stage.numWorkersPerTask)
	}

	for i := range p.stages {
		sIdx := i
		stage := &p.stages[i]

		// nil on the first iteration:
		stage.inputCh = nextInputCh

		for j := 0; j < int(stage.numWorkersPerTask); j++ {
			g.Go(func() error {
				for {
					p.debugPrintf("stage %d reading from %v\n", sIdx, stage.inputCh)

					var input interface{}
					if stage.inputCh != nil {
						input = <-stage.inputCh
					}

					job, err := stage.worker(input)
					if err != nil {
						return err
					}

					// The last stage doesn't write anything
					if sIdx == len(p.stages)-1 {
						continue
					}

					p.debugPrintf("stage %d writing to %v\n", sIdx, stage.outputCh)
					stage.outputCh <- job
				}
			})
		}

		// Create the input channel of the next stage:
		nextInputCh = stage.outputCh
	}

	return g.Wait()
}

func (p Pipeline) debugPrintf(format string, args ...interface{}) {
	if p.Debug {
		fmt.Printf(format, args...)
	}
}

// Stage ...
type Stage struct {
	name              string
	numWorkersPerTask NumWorkers
	worker            workerType

	inputCh  chan interface{}
	outputCh chan interface{}
}

// NewStage ...
func NewStage(name string, numWorkersPerTask NumWorkers, worker workerType) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return Stage{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		worker:            worker,
	}
}

type workerType func(job interface{}) (interface{}, error)
