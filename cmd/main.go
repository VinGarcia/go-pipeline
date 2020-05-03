package main

import (
	"fmt"
	"time"

	"github.com/vingarcia/go-pipeline"
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
		pipeline.NewStage("loading", pipeline.NumWorkers(1), func(_ interface{}) (interface{}, error) {
			time.Sleep(100 * time.Millisecond)

			job := msgGenerator()

			fmt.Println("loading", job)

			return job, nil
		}),

		// Fan-out with 2 tasks:
		pipeline.NewStage("dedup", pipeline.NumWorkers(1), func(job interface{}) (interface{}, error) {
			fmt.Println("deduping", job)
			return job.(string) + " deduped", nil
		}),

		pipeline.NewStage("rate-control", pipeline.NumWorkers(1), func(job interface{}) (interface{}, error) {

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

		pipeline.NewStage("sending", pipeline.NumWorkers(3), func(job interface{}) (interface{}, error) {
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
