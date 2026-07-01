package scheduler

import (
	"sync"
	"time"
)

type Node struct {
	tasks   []*Job
	workers []*Worker
	mu      sync.Mutex
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

		node.mu.Lock()
		currEpoch := currJob.epoch
		worker.lastBeat = time.Now()

		worker.currJob = currJob
		worker.currJob.state = StateRunning
		node.mu.Unlock()

		endTime := time.Now().Add(currJob.duration)

		// Loop through times before endTime (task finishes)
		// if worker is dead, return, else send a heartbeat.
		for time.Now().Before(endTime) {
			node.mu.Lock()
			if !worker.alive {
				node.mu.Unlock()
				return
			}
			worker.lastBeat = time.Now()
			node.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}

		node.mu.Lock()
		if currJob.epoch == currEpoch {
			currJob.state = StateDone
			worker.currJob = nil
			node.wg.Done()
		}
		node.mu.Unlock()
	}
}

func (node *Node) start() {
	for i := range node.workers {
		go node.runWorker(node.workers[i])
	}
	go node.monitor(50*time.Millisecond, 10*time.Millisecond)
}

func (node *Node) monitor(timeout time.Duration, interval time.Duration) {
	for {
		node.mu.Lock()
		for i := range node.workers {
			lastBeat := node.workers[i].lastBeat
			if time.Since(lastBeat) > timeout && node.workers[i].currJob != nil {
				currJob := node.workers[i].currJob

				node.tasks = append(node.tasks, currJob)
				currJob.epoch++
				currJob.state = StatePending
				node.workers[i].currJob = nil
			}
		}
		node.mu.Unlock()
		time.Sleep(interval)
	}
}
