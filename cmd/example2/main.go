package main

import (
	"fmt"
	"time"

	"github.com/vingarcia/go-pipeline"
	"github.com/vingarcia/go-pipeline/async"
)

func main() {
	limiter := time.Tick(time.Second / 4)

	msgCounter := 0
	msgGenerator := func() string {
		msgCounter++
		return fmt.Sprint("message ", msgCounter)
	}

	startTime := time.Now()
	jobsProcessed := 0

	pipeline := pipeline.New(
		pipeline.NewStage("loading", async.TaskForce(1), func(_ interface{}) (interface{}, error) {
			time.Sleep(100 * time.Millisecond)

			job := msgGenerator()

			fmt.Println("loading", job)

			return job, nil
		}),

		// Fan-out with 2 tasks:
		pipeline.NewFanoutStage("save-and-dedup", async.TaskForce(1),
			func(job interface{}) (interface{}, error) {
				fmt.Println("deduping", job)
				return job.(string) + " deduped", nil
			},
			func(job interface{}) (interface{}, error) {
				fmt.Println("saving-to-database", job)
				return job.(string) + " saved", nil
			},
		).FaninRule(func(results []interface{}) (interface{}, error) {
			return results[0], nil
		}),

		pipeline.NewStage("rate-control", async.TaskForce(1), func(job interface{}) (interface{}, error) {

			<-limiter

			fmt.Println("rate-controlling", job)

			if time.Since(startTime) > 2*time.Second {
				fmt.Println()
				fmt.Printf("Processing rate: %f\n", float64(jobsProcessed)/time.Since(startTime).Seconds())
				fmt.Println()
				startTime = time.Now()
				jobsProcessed = 0
			}
			jobsProcessed++

			return job, nil
		}),

		pipeline.NewStage("sending", async.TaskForce(3), func(job interface{}) (interface{}, error) {
			time.Sleep(300 * time.Millisecond)

			fmt.Printf("Sending message `%s`\n", job.(string))

			return nil, nil
		}),
	)

	pipeline.Debug = true
	err := pipeline.Start()
	if err != nil {
		fmt.Println(err, err.Error())
	}
}
