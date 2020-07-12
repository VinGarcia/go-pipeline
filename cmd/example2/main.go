package main

import (
	"fmt"
	"time"

	thread "github.com/vingarcia/go-pipeline"
	p "github.com/vingarcia/go-pipeline/pipeline"
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

	p.Debug = true
	pipeline := p.New(
		p.NewStage("loading", thread.TaskForce(1), func(_ interface{}) (interface{}, error) {
			time.Sleep(100 * time.Millisecond)

			job := msgGenerator()

			fmt.Println("loading", job)

			return job, nil
		}),

		// Fan-out with 2 tasks:
		p.NewFanoutStage("save-and-dedup", thread.TaskForce(1),
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

		p.NewStage("rate-control", thread.TaskForce(1), func(job interface{}) (interface{}, error) {

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

		p.NewStage("sending", thread.TaskForce(3), func(job interface{}) (interface{}, error) {
			time.Sleep(300 * time.Millisecond)

			fmt.Printf("Sending message `%s`\n", job.(string))

			return nil, nil
		}),
	)

	err := pipeline.Start()
	if err != nil {
		fmt.Println(err, err.Error())
	}
}
