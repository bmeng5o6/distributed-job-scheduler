package scheduler

type Job struct {
	cleared bool
	task    string
}

func newJob(task string) *Job {
	return &Job{
		cleared: false, task: task,
	}
}
