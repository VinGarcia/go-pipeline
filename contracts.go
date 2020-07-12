package threads

import (
	"context"
)

// Task describes an asyncronous task that must be performed
// on one of the Stages of the Pipeline or on concurrently
// on the Fanout model
type Task func(job interface{}) (interface{}, error)

// Num is used to describe the number
// of workers that should perform a single task
type Num int

// Pipeline interface abstracts the concept of a Pipeline
// that blocks after starting and only returns if its
// context is cancelled or an error occurs
type Pipeline interface {
	Start() error
	StartWithContext(ctx context.Context) error
}

// Stage describes an stage of the Pipeline
//
// Each stage of the pipeline processes the jobs serially
// and when there are multiple tasks on a single stage they
// process jobs concurrently among them
type Stage interface {
	GetName() string
	NumThreads() int
	GetWorker() Task
}
