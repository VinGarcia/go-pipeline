## Simple Go Pipeline

This implementation allows you to worry about what each stage does
instead of worrying about how to sincronize them.

The abstraction contains 3 layers:

1. The Pipeline itself
2. The Stages of the pipeline that run in serial order
3. The Tasks of each Stage, that allows a single stage to run several tasks in parallel

There are 2 types of stages:

1. Stages with a single Task
2. Stages with more than 1 Task

For stages with one task the return value of this Task is always
the input argument of the tasks on the next Stage.

For stages with multiple tasks, the return value of all these tasks
are unified into a list of type `[]interface{}`.
This list is then provided as input for the tasks of the next stage.

## Usage Example

```Go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vingarcia/go-threads"
	"github.com/vingarcia/go-threads/pipeline"
)

func main() {
	i := 0
	pipeline := pipeline.New(
		pipeline.NewStage("coolNameForStage1", threads.Num(1), func(_ interface{}) (interface{}, error) {
			// The argument of the stage 0 tasks is nil, so we ignore it  ^

			i++
			return fmt.Sprint("job ", i), nil
		}),

		pipeline.NewStage("coolNameForStage3", threads.Num(1), func(job interface{}) (interface{}, error) {
			// The job received is always the return value of the last stage,
			// in this case its "new job" so cast it into a string:
			strJob := job.(string) + ":passing by stage 3"

			return strJob, nil
		}),

		// This is a fanout stage:
		pipeline.NewFanoutStage("myFanoutStage", threads.Num(1),
			// It has multiple tasks instead of just one:
			func(job interface{}) (interface{}, error) {
				return job.(string) + ":fanout task 1", nil
			},
			func(job interface{}) (interface{}, error) {
				return job.(string) + ":fanout task 2", nil
			},
		).FaninRule(func(results []interface{}) (interface{}, error) {
			// After a FanoutStage you'll need to fanin your results,
			// in this case we are discarding the result
			// of the second task:
			return results[0], nil
		}),

		pipeline.NewStage(
			// Since this is a very slow stage we can add extra Num:
			"verySlowStage", threads.Num(4),
			// This will create 4 workers running the task bellow,
			// so everytime a new job arrives from the faster stages
			// there is always a worker ready to read it:
			func(job interface{}) (interface{}, error) {

				time.Sleep(1 * time.Second)
				fmt.Println(job) // "job 1:passing by stage 3:fanout task 1"

				if i == 10 {
					return nil, errors.New("This error will stop the pipeline")
				}

				// The return value of the last stage is ignored,
				// so we can set it to nil:
				return nil, nil
			},
		),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := pipeline.StartWithContext(ctx) // Or just pipeline.Start() if you won't use a context.
	fmt.Println(err)

	return
}
```
