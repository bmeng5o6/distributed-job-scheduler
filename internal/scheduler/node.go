package scheduler

import (
	"errors"
)

type Node struct {
	tasks   []*Job
	workers []*Worker
}

func newNode(tasks []*Job, workers []*Worker) *Node {
	return &Node{
		tasks: tasks, workers: workers,
	}
}

func scheduleTasks(node *Node) error {
	if len(node.tasks) == 0 {
		return errors.New("No jobs in tasks")
	}

	for len(node.tasks) > 0 {
		currTask := node.tasks[0]
		assigned := false
		for i := range node.workers {
			if node.workers[i].currJob == nil {
				node.workers[i].currJob = currTask
				assigned = true
				runJob(node.workers[i])

				break
			}
		}

		if !assigned {
			return errors.New("No workers available")
		}

		node.tasks = node.tasks[1:]
	}

	return nil
}
