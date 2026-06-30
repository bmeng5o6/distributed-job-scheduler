package scheduler

import (
	"log"
	"sync"
)

type Node struct {
	tasks   []*Job
	workers []*Worker
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

func newNode(tasks []*Job, workers []*Worker) *Node {
	return &Node{
		tasks: tasks, workers: workers,
	}
}

func (node *Node) pullJob() *Job {
	node.mu.Lock()
	defer node.mu.Unlock()

	if len(node.tasks) == 0 {
		return nil
	}

	job := node.tasks[0]
	node.tasks = node.tasks[1:]
	return job
}

func (node *Node) runWorker(worker *Worker) {
	for {
		currJob := node.pullJob()
		if currJob == nil {
			return
		}

		worker.currJob = currJob
		worker.currJob.state = StateRunning

		log.Println("running task")

		worker.currJob.state = StateDone
		worker.currJob = nil
		node.wg.Done()
	}
}

func (node *Node) start() {
	for i := range node.workers {
		go node.runWorker(node.workers[i])
	}
}
