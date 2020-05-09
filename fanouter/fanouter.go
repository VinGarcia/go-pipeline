package fanouter

import (
	"context"

	"github.com/vingarcia/go-pipeline/workerpool"
)

// TaskType describes one task that must be performed during the fanout
type TaskType func(job interface{}) (interface{}, error)

// Fan holds a set of workers that run concurrently
// to process a single job everytime the Fanout() function is called.
type Fan struct {
	ctx context.Context

	pool *workerpool.WorkerPool

	jobs      chan interface{}
	batchesCh chan chan fanJob
	outputCh  chan []interface{}

	err error
}

// New creates a new Fan instance
func New(ctx context.Context, tasks ...TaskType) Fan {
	f := Fan{
		ctx:       ctx,
		jobs:      make(chan interface{}),
		batchesCh: make(chan chan fanJob),
		outputCh:  make(chan []interface{}),
	}

	// Start the actual workers:
	f.pool = workerpool.NewWorkerPool(f.ctx, len(tasks)+2)
	// Start the worker responsible for distributing the jobs:
	f.pool.Go(fanoutWorker(f.ctx, f.jobs, f.batchesCh, tasks, f.pool))
	// Start the worker responsible for collecting the results:
	f.pool.Go(faninWorker(f.ctx, len(tasks), f.batchesCh, f.outputCh))

	return f
}

// Fanout sends a job to be processed by each of the tasks
// registered by the New() function
//
// The output is returned in the form of a list where each
// position is the return value of one of the tasks in
// respective order.
func (f Fan) Fanout(job interface{}) ([]interface{}, error) {
	f.jobs <- job
	return <-f.outputCh, f.err
}

// Wait awaits until the first error happens on one the jobs
// or all the tasks finish.
func (f Fan) Wait() error {
	return f.pool.Wait()
}

type fanJob struct {
	taskIdx int
	job     interface{}
}

func fanoutWorker(
	ctx context.Context,
	jobs chan interface{},
	batchesCh chan chan fanJob,
	tasks []TaskType,
	pool *workerpool.WorkerPool,
) func() error {
	return func() error {
		var job interface{}
		for {
			select {
			case <-ctx.Done():
			case job = <-jobs:
			}

			batch := make(chan fanJob, len(tasks))
			for i := range tasks {
				task := tasks[i]
				taskIdx := i

				pool.Go(func() error {
					resp, err := task(job)
					if err != nil {
						return err
					}

					batch <- fanJob{
						taskIdx: taskIdx,
						job:     resp,
					}
					return nil
				})
			}
		}

		return nil
	}
}

func faninWorker(
	ctx context.Context,
	numWorkers int,
	batchesCh chan chan fanJob,
	outputCh chan []interface{},
) func() error {
	return func() error {
		var batch chan fanJob
		for {
			select {
			case <-ctx.Done():
				return nil
			case batch = <-batchesCh:
			}

			output := make([]interface{}, numWorkers)
			for i := 0; i < numWorkers; i++ {
				fanJob := <-batch
				output[fanJob.taskIdx] = fanJob.job
			}
			outputCh <- output
		}
		return nil
	}
}
