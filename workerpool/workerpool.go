package workerpool

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type workRequest func() error

// WorkerPool ...
type WorkerPool struct {
	numWorkers int
	requests   chan workRequest
	err        error

	cancelAll func()
	group     *errgroup.Group
}

// NewWorkerPool ...
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

	// Stop all other workers:
	g.Go(func() error {
		defer close(pool.requests)
		<-ctx.Done()
		return nil
	})

	return &pool
}

// AddWorkers ...
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

// NumWorkers ...
func (pool WorkerPool) NumWorkers() int {
	return pool.numWorkers
}

// Go ...
func (pool WorkerPool) Go(work func() error) {
	pool.requests <- work
}

// Wait ...
func (pool WorkerPool) Wait() error {
	return pool.group.Wait()
}
