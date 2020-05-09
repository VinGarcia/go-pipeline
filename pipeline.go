package pipeline

import (
	"context"
	"fmt"

	"github.com/vingarcia/go-pipeline/fanouter"
	"golang.org/x/sync/errgroup"
)

// TaskForce is used to describe the number
// of workers per task a given stage should use
type TaskForce int

// Pipeline organizes several goroutines to process a
// number of user defined tasks in the form of a pipeline.
//
// Each stage of this pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently
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
	return p.StartWithContext(context.TODO())
}

// StartWithContext ...
func (p Pipeline) StartWithContext(ctx context.Context) error {
	var nextInputCh chan interface{}
	var g errgroup.Group

	for i := range p.stages {
		stage := &p.stages[i]

		// nil on the first iteration:
		stage.inputCh = nextInputCh

		for j := 0; j < int(stage.numWorkersPerTask); j++ {
			if len(stage.tasks) == 1 {
				g.Go(p.stageWorker(ctx, i, stage, stage.inputCh, stage.tasks[0]))
				continue
			}

			fan := fanouter.New(ctx, stage.tasks...)
			g.Go(p.stageWorker(ctx, i, stage, stage.inputCh, func(job interface{}) (interface{}, error) {
				return fan.Fanout(job)
			}))
		}

		// Create the input channel of the next stage:
		nextInputCh = stage.outputCh
	}

	return g.Wait()
}

// Stage ...
type Stage struct {
	name  string
	tasks []fanouter.TaskType

	inputCh  chan interface{}
	outputCh chan interface{}

	numWorkersPerTask TaskForce
}

// NewStage ...
func NewStage(name string, numWorkersPerTask TaskForce, task fanouter.TaskType) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if numWorkersPerTask < 1 {
		numWorkersPerTask = 1
	}

	return Stage{
		name:              name,
		numWorkersPerTask: numWorkersPerTask,
		tasks:             []fanouter.TaskType{task},
		outputCh:          make(chan interface{}, numWorkersPerTask),
	}
}

// NewFanoutStage ...
func NewFanoutStage(name string, numWorkersPerTask TaskForce, tasks ...fanouter.TaskType) Stage {
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

func (p Pipeline) stageWorker(
	ctx context.Context,
	stageIdx int,
	stage *Stage,
	inputCh chan interface{},
	task fanouter.TaskType,
) func() error {
	return func() error {
		var job interface{}
		for {
			p.debugPrintf("stage `%s` (n%d) reading from %v\n", stage.name, stageIdx, stage.inputCh)

			if stageIdx > 0 {
				job = <-stage.inputCh
			}

			resp, err := task(job)
			if err != nil {
				return err
			}

			// The last stage doesn't write anything
			if stageIdx == len(p.stages)-1 {
				continue
			}

			p.debugPrintf("stage `%s` (n%d) writing to %v\n", stage.name, stageIdx, stage.outputCh)
			stage.outputCh <- resp
		}
	}
}

func (p Pipeline) debugPrintf(format string, args ...interface{}) {
	if p.Debug {
		fmt.Printf(format, args...)
	}
}
