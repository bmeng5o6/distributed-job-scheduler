package scheduler

import (
	"sync"
)

type Worker struct {
	name    string
	currJob *Job
	mu      sync.Mutex
	cnt     int32
}

func newWorker(name string) *Worker {
	return &Worker{
		name: name, currJob: nil, cnt: 0,
	}
}
