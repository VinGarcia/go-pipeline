package async

// Task describes an asyncronous task that must be performed
// on one of the Stages of the Pipeline or on concurrently
// on the Fanout model
type Task func(job interface{}) (interface{}, error)

// TaskForce is used to describe the number
// of workers that should perform a single task
type TaskForce int
