package main

import (
	"fmt"
	"go-mod/pipe"
	"time"
)

func main() {
	limiter := time.Tick(time.Second / 10)

	pipeline := pipe.New(
		pipe.NewStage("loading",
			pipe.NewTask(1, func(_ interface{}) (interface{}, error) {
				time.Sleep(1 * time.Second)

				return "message", nil
			}),
		),

		pipe.NewStage("dedup",
			pipe.NewTask(1, func(job interface{}) (interface{}, error) {
				return job.(string) + " deduped", nil
			}),
		),

		pipe.NewStage("rate-control",
			pipe.NewTask(1, func(job interface{}) (interface{}, error) {

				<-limiter

				return job, nil
			}),
		),

		pipe.NewStage("sending",
			pipe.NewTask(1, func(job interface{}) (interface{}, error) {
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
