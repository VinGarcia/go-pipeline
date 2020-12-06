package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vingarcia/go-threads"
	p "github.com/vingarcia/go-threads/pipeline"
)

func main() {
	i := 0
	p.Debug = true
	pipeline := p.New(
		p.Stage{
			Name:    "coolNameForStage1",
			Threads: 1,
			Task: func(_ interface{}) (interface{}, error) {
				i++
				return fmt.Sprint("job ", i), nil
			},
		},

		p.Stage{
			Name:    "coolNameForStage2",
			Threads: 1,
			Task: func(job interface{}) (interface{}, error) {
				strJob := job.(string) + ":passing by stage 3"

				return strJob, nil
			},
		},

		// This is a fanout stage:
		p.FanoutStage{
			Name:           "myFanoutStage",
			ThreadsPerTask: 1,
			Tasks: []threads.Task{
				func(job interface{}) (interface{}, error) {
					return job.(string) + ":fanout task 1", nil
				},
				func(job interface{}) (interface{}, error) {
					return job.(string) + ":fanout task 2", nil
				},
			},
			FaninRule: func(results []interface{}) (interface{}, error) {
				return results[0], nil
			},
		},

		p.Stage{
			Name:    "verySlowStage",
			Threads: 4,
			Task: func(job interface{}) (interface{}, error) {
				time.Sleep(1 * time.Second)
				fmt.Println(job) // "job 1:passing by stage 3:fanout task 1"

				if i == 10 {
					return nil, errors.New("This error will stop the pipeline")
				}

				return nil, nil
			},
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := pipeline.StartWithContext(ctx) // Or just pipeline.Start() if you won't use a context.
	fmt.Println(err)

	return
}
