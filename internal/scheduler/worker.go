package scheduler

import (
	"time"
)

type Worker struct {
	name     string
	currJob  *Job
	lastBeat time.Time
	alive    bool
}

func newWorker(name string) *Worker {
	return &Worker{
		name: name, currJob: nil, alive: true,
	}
}
