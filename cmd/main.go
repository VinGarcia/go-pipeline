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
		pipeline.NewStage("loading", 1,
			pipeline.NewTask(func(_ interface{}) (interface{}, error) {
				time.Sleep(100 * time.Millisecond)

				job := msgGenerator()

				fmt.Println("loading", job)

				return job, nil
			}),
		),

		// Fan-out with 2 tasks:
		pipeline.NewStage("dedup", 1,
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("deduping", job)
				return job.(string) + " deduped", nil
			}),

			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("saving to db", job)
				return nil, nil
			}),
		),

		pipeline.NewStage("rate-control", 1,
			pipeline.NewTask(func(jobs interface{}) (interface{}, error) {
				// Since the last stage was a fan-out, in this stage we receive
				// multiple return values. In this case we'll only use the first one:
				job := jobs.([]interface{})[0].(string)

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
		),

		pipeline.NewStage("sending", 3,
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				time.Sleep(300 * time.Millisecond)

				fmt.Printf("Sending message `%s`\n", job.(string))

				return nil, nil
			}),
		),
	)

	err := pipeline.Start()
	if err != nil {
		fmt.Println(err, err.Error())
	}
}
