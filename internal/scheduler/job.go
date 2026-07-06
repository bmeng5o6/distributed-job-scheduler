package scheduler

import (
	"time"
)

type Job struct {
	state     State
	task      string
	duration  time.Duration
	epoch     int
	attempt   int
	failCount int // number of times a job needs to be run to succeed. This is an attempt at running an actual job.
	readyAt   time.Time
}

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

func newJob(task string, duration time.Duration, failCount int) *Job {
	return &Job{
		state: StatePending, task: task, duration: duration, failCount: failCount,
	}
}
