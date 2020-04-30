package main

import (
	"fmt"
	"time"

	"github.com/vingarcia/go-pipeline"
)

func main() {
	limiter := time.Tick(time.Second / 10)
	c := 0

	pipeline := pipeline.New(
		pipeline.NewStage("loading",
			pipeline.NewTask(func(_ interface{}) (interface{}, error) {
				time.Sleep(1 * time.Second)

				c++
				job := fmt.Sprint("message ", c)

				fmt.Println("loading", job)

				return job, nil
			}),
		),

		// Fan-out with 2 tasks:
		pipeline.NewStage("dedup",
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("deduping", job)
				return job.(string) + " deduped", nil
			}),
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				fmt.Println("saving to db", job)
				return nil, nil
			}),
		),

		pipeline.NewStage("rate-control",
			pipeline.NewTask(func(jobs interface{}) (interface{}, error) {
				// Since the last stage was a fan-out, in this stage we receive
				// multiple return values. In this case we'll only use the first one:
				job := jobs.([]interface{})[0].(string)

				<-limiter

				fmt.Println("rate-controlling", job)

				return job, nil
			}),
		),

		pipeline.NewStage("sending",
			pipeline.NewTask(func(job interface{}) (interface{}, error) {
				time.Sleep(3 * time.Second)

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
