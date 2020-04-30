package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Pipeline ...
type Pipeline struct {
	stages []Stage

	started bool
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

	for i := range p.stages {
		sIdx := i
		stage := p.stages[i]

		// nil on the first iteration:
		stage.inputCh = nextInputCh

		// Fan-out:
		g.Go(func() error {
			for {
				var job interface{}
				if stage.inputCh != nil {
					job = <-stage.inputCh
				}

				for i := range stage.tasks {
					task := stage.tasks[i]
					task.inputCh <- job
				}
			}
		})

		// Tasks:
		for i := range stage.tasks {
			task := stage.tasks[i]

			g.Go(func() error {
				for {
					job, err := task.worker(<-task.inputCh)
					if err != nil {
						return err
					}

					task.outputCh <- job
				}
			})
		}

		// Fan-in:
		g.Go(func() error {
			for {
				var jobs []interface{}
				for i := range stage.tasks {
					task := stage.tasks[i]
					jobs = append(jobs, <-task.outputCh)
				}

				// The last stage doesn't write anything
				if sIdx == len(p.stages)-1 {
					continue
				}

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

// Stage ...
type Stage struct {
	name     string
	tasks    []Task
	inputCh  chan interface{}
	outputCh chan interface{}
}

// NewStage ...
func NewStage(name string, tasks ...Task) Stage {
	return Stage{
		name:     name,
		tasks:    tasks,
		outputCh: make(chan interface{}),
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
		worker:   worker,
		inputCh:  make(chan interface{}),
		outputCh: make(chan interface{}),
	}
}

type workerType func(job interface{}) (interface{}, error)
