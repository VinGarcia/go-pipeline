package workerpool

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type WorkRequest func() error

type WorkerPool struct {
	numWorkers int
	requests   chan WorkRequest
	err        error

	cancelAll func()
	group     *errgroup.Group
}

func NewWorkerPool(ctx context.Context, numWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	pool := WorkerPool{
		requests:  make(chan WorkRequest),
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

func (pool WorkerPool) NumWorkers() int {
	return pool.numWorkers
}

func (pool WorkerPool) Go(work func() error) {
	pool.requests <- work
}

func (pool WorkerPool) Wait() error {
	return pool.group.Wait()
}
