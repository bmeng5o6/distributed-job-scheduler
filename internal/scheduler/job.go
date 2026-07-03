package scheduler

import (
	"time"
)

type Job struct {
	state   State
	task    string
	epoch   int
	attempt int
	run     func() error
}

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

func newJob(task string, duration time.Duration, run func() error) *Job {
	return &Job{
		state: StatePending, task: task, run: run,
	}
}
