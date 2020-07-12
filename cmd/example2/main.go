package main

import (
	"fmt"
	"time"

	"github.com/vingarcia/go-threads"
	p "github.com/vingarcia/go-threads/pipeline"
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
		p.Stage{
			Name:    "loading",
			Threads: 1,
			Task: func(_ interface{}) (interface{}, error) {
				time.Sleep(100 * time.Millisecond)

				job := msgGenerator()

				fmt.Println("loading", job)

				return job, nil
			},
		},

		// Fan-out with 2 tasks:
		p.FanoutStage{
			Name:           "save-and-dedup",
			ThreadsPerTask: 1,

			Tasks: []threads.Task{
				func(job interface{}) (interface{}, error) {
					fmt.Println("deduping", job)
					return job.(string) + " deduped", nil
				},
				func(job interface{}) (interface{}, error) {
					fmt.Println("saving-to-database", job)
					return job.(string) + " saved", nil
				},
			},

			// Optional, the implementation below is the default
			// implementation that would be used if this field was omitted:
			FaninRule: func(results []interface{}) (interface{}, error) {
				return results[0], nil
			},
		},

		p.Stage{
			Name:    "rate-control",
			Threads: 1,
			Task: func(job interface{}) (interface{}, error) {

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
			},
		},

		p.Stage{
			Name:    "sending",
			Threads: 3,
			Task: func(job interface{}) (interface{}, error) {
				time.Sleep(300 * time.Millisecond)

				fmt.Printf("Sending message `%s`\n", job.(string))

				return nil, nil
			},
		},
	)

	err := pipeline.Start()
	if err != nil {
		fmt.Println(err, err.Error())
	}
}
