package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	thread "github.com/vingarcia/go-pipeline"
	p "github.com/vingarcia/go-pipeline/pipeline"
)

func main() {
	i := 0
	p.Debug = true
	pipeline := p.New(
		p.NewStage("coolNameForStage1", thread.TaskForce(1), func(_ interface{}) (interface{}, error) {
			// The argument of the stage 0 tasks is nil, so we ignore it  ^

			i++
			return fmt.Sprint("job ", i), nil
		}),

		p.NewStage("coolNameForStage3", thread.TaskForce(1), func(job interface{}) (interface{}, error) {
			// The job received is always the return value of the last stage,
			// in this case its "new job" so cast it into a string:
			strJob := job.(string) + ":passing by stage 3"

			return strJob, nil
		}),

		// This is a fanout stage:
		p.NewFanoutStage("myFanoutStage", thread.TaskForce(1),
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

		p.NewStage(
			// Since this is a very slow stage we can add extra TaskForce:
			"verySlowStage", thread.TaskForce(4),
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
