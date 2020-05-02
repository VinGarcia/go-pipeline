package pipeline

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

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
		stage.outputCh = make(chan interface{}, stage.workersPerTask)
		for j := range stage.tasks {
			task := &stage.tasks[j]
			task.inputCh = make(chan interface{}, stage.workersPerTask)
			task.outputCh = make(chan interface{})
		}
	}

	for i := range p.stages {
		sIdx := i
		stage := &p.stages[i]

		// nil on the first iteration:
		stage.inputCh = nextInputCh

		// Fan-out:
		g.Go(func() error {
			for {
				var job interface{}
				if stage.inputCh != nil {
					p.debugPrintf("stage %d fanout reading from %v\n", sIdx, stage.inputCh)
					job = <-stage.inputCh
				}

				for i := range stage.tasks {
					idx := i
					task := stage.tasks[i]
					p.debugPrintf("stage %d fanout writing to task %d on chan %v\n", sIdx, idx, task.inputCh)
					task.inputCh <- job
				}
			}
		})

		// Tasks:
		for i := range stage.tasks {
			task := &stage.tasks[i]

			for j := 0; j < stage.workersPerTask; j++ {
				workerIdx := j
				g.Go(func() error {
					for {
						p.debugPrintf("stage %d task %d reading from %v\n", sIdx, workerIdx, task.inputCh)
						job, err := task.worker(<-task.inputCh)
						if err != nil {
							return err
						}

						p.debugPrintf("stage %d task %d writing to %v\n", sIdx, workerIdx, task.outputCh)
						task.outputCh <- job
					}
				})
			}
		}

		// Fan-in:
		g.Go(func() error {
			for {
				var jobs []interface{}
				for i := range stage.tasks {
					idx := i
					task := stage.tasks[i]
					p.debugPrintf("stage %d fanin reading from task %d on chan %v\n", sIdx, idx, task.inputCh)
					jobs = append(jobs, <-task.outputCh)
				}

				// The last stage doesn't write anything
				if sIdx == len(p.stages)-1 {
					continue
				}

				p.debugPrintf("stage %d fanin writing to %v\n", sIdx, stage.outputCh)
				if len(stage.tasks) == 1 {
					stage.outputCh <- jobs[0]
				} else {
					stage.outputCh <- jobs
				}
			}
		})

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
	name           string
	workersPerTask int
	tasks          []Task

	inputCh  chan interface{}
	outputCh chan interface{}
}

// NewStage ...
func NewStage(name string, workersPerTask int, tasks ...Task) Stage {
	// Ignore nonsence arguments
	// so we don't have to return error:
	if workersPerTask < 1 {
		workersPerTask = 1
	}

	return Stage{
		name:           name,
		workersPerTask: workersPerTask,
		tasks:          tasks,
	}
}

// Task ...
type Task struct {
	instances int
	worker    workerType

	inputCh  chan interface{}
	outputCh chan interface{}
}

// NewTask ...
func NewTask(worker workerType) Task {
	return Task{
		worker: worker,
	}
}

type workerType func(job interface{}) (interface{}, error)
