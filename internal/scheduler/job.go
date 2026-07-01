package scheduler

import (
	"time"
)

type Job struct {
	state    State
	task     string
	epoch    int
	duration time.Duration
}

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

func newJob(task string, duration time.Duration) *Job {
	return &Job{
		state: StatePending, task: task, duration: duration,
	}
}
