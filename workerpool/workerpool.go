package workerpool

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type workRequest func() error

// WorkerPool manages a number of goroutines that can accept
// jobs and perform them without allocating new goroutines
// for each job.
type WorkerPool struct {
	numWorkers int
	requests   chan workRequest
	err        error

	cancelAll func()
	group     *errgroup.Group
}

// NewWorkerPool creates a new WorkerPool with numWorkers
// goroutines ready to receive jobs
func NewWorkerPool(ctx context.Context, numWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	pool := WorkerPool{
		requests:  make(chan workRequest),
		err:       nil,
		cancelAll: cancel,
		group:     g,
	}

	pool.AddWorkers(numWorkers)

	// Stop all other workers if the context is cancelled:
	g.Go(func() error {
		defer close(pool.requests)
		<-ctx.Done()
		return nil
	})

	return &pool
}

// AddWorkers allocates numWorkers new goroutines
// for processing incomming jobs
func (pool WorkerPool) AddWorkers(numWorkers int) {
	pool.numWorkers += numWorkers
	for i := 0; i < numWorkers; i++ {
		pool.group.Go(func() error {
			for work := range pool.requests {
				err := work()
				if err != nil {
					pool.err = err
					pool.cancelAll()
					return nil
				}
			}

			return nil
		})
	}
}

// NumWorkers returns the current number of workers
func (pool WorkerPool) NumWorkers() int {
	return pool.numWorkers
}

// Go adds a job to the execution queue, if the job returns
// error the entire WorkerPool starts a graceful shutdown
// and returns the received error.
func (pool WorkerPool) Go(work func() error) {
	pool.requests <- work
}

// Wait blocks until an error happens on one of the jobs
// or the WorkerPool receives a signal to shutdown either
// by cancelling the input ctx or by calling Cancel()
func (pool WorkerPool) Wait() error {
	return pool.group.Wait()
}

// Close sends a signsl to stop all goroutines
// and then wait for them to finish shutting down
func (pool WorkerPool) Close() error {
	pool.cancelAll()
	return pool.group.Wait()
}
