package scheduler

type Worker struct {
	currJob *Job
}

func newWorker() *Worker {
	return &Worker{
		currJob: nil,
	}
}
