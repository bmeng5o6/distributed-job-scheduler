package scheduler

import (
	"errors"
	"log"
)

type Worker struct {
	name    string
	currJob *Job
}

func newWorker(name string) *Worker {
	return &Worker{
		name: name, currJob: nil,
	}
}

func runJob(worker *Worker) error {
	if worker.currJob == nil {
		return errors.New("Worker had no job, runJob called")
	}

	log.Println("Running Job.")
	worker.currJob.state = StateRunning

	log.Println("Completed Job.")
	worker.currJob.state = StateDone
	worker.currJob = nil

	return nil
}
