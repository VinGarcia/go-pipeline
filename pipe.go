package pipe

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

	var g errgroup.Group

	inputCh := make(chan interface{})
	// This initial channel is closed so all
	// attempts to read return nil but doesn't block:
	close(inputCh)

	for sIdx, stage := range p.stages {

		// Fan-out:
		g.Go(func() error {
			for {
				job := <-inputCh
				for _, task := range stage.tasks {
					task.inputCh <- job
				}
			}
		})

		// Tasks:
		for _, task := range stage.tasks {
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
				for _, task := range stage.tasks {
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
		inputCh = make(chan interface{})
	}

	return nil
}

// Stage ...
type Stage struct {
	name     string
	tasks    []Task
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
func NewTask(numInstances int, worker workerType) Task {
	return Task{
		instances: numInstances,
		worker:    worker,
		inputCh:   make(chan interface{}, 1),
		outputCh:  make(chan interface{}),
	}
}

type workerType func(job interface{}) (interface{}, error)
