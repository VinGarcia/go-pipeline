package pool

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type workRequest func() error

// Pool manages a number of goroutines that can accept
// jobs and perform them without allocating new goroutines
// for each job.
type Pool struct {
	numWorkers int
	requests   chan workRequest
	err        error

	cancelAll func()
	group     *errgroup.Group
}

// New creates a new Pool of workers with numWorkers
// goroutines ready to receive jobs
func New(ctx context.Context, numWorkers int) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	pool := Pool{
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
func (pool Pool) AddWorkers(numWorkers int) {
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
func (pool Pool) NumWorkers() int {
	return pool.numWorkers
}

// Go adds a job to the execution queue, if the job returns
// error the entire Pool starts a graceful shutdown
// and returns the received error.
func (pool Pool) Go(work func() error) {
	pool.requests <- work
}

// Wait blocks until an error happens on one of the jobs
// or the Pool receives a signal to shutdown either
// by cancelling the input ctx or by calling Cancel()
func (pool Pool) Wait() error {
	return pool.group.Wait()
}

// Close sends a signsl to stop all goroutines
// and then wait for them to finish shutting down
func (pool Pool) Close() error {
	pool.cancelAll()
	return pool.group.Wait()
}
