package scheduler

type Job struct {
	state State
	task  string
}

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

func newJob(task string) *Job {
	return &Job{
		state: StatePending, task: task,
	}
}
