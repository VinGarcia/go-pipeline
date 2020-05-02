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
	"fmt"
	"os"

	"github.com/vingarcia/go-pipeline"
)

func main() {
	pipeline := pipeline.New(
		pipeline.NewStage("coolNameForStage1", 1,
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("The argument of the tasks on stage 0 are always nil", job)

				return "new job", nil
			}),
		),

		pipeline.NewStage("coolNameForStage2 which is a fanout stage", 1,
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				strJob := job.(string)
				fmt.Printf("The argument on subsequent stages must be cast to its correct type: %#v", strJob)

				return strJob + " 1", nil
			}),
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				strJob := job.(string)
				fmt.Println("When you have 2 tasks on the same stage you'll receive a list on the next stage")

				return strJob + " 2", nil
			}),
		),

		pipeline.NewStage("coolNameForStage3", 1,
			pipeline.NewTask(func(rawValues interface{}) (interface{}, error) {
				jobs := rawValues.([]interface{})
				fmt.Printf("And you'll have to cast it into a list of values: %#v\n", jobs)

				return jobs[0].(string) + jobs[1].(string), nil
			}),
		),

		pipeline.NewStage("coolNameForStage4", 3,
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("The return value of the last stage is discarded")
				fmt.Println("So if you want to use it, send it somewhere")

				// For preventing an infinity loop on your hello world example:
				os.Exit(0)

				return "this string will be discarded", nil
			}),
		),
	)

	err := pipeline.Start()
	fmt.Println(err)

	return
}
```
