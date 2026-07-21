package scheduler

import (
	"container/heap"
	"sync"
	"time"
)

type Node struct {
	tasks       jobHeap
	deadTasks   []*Job
	workers     []*Worker
	mu          sync.Mutex
	cond        *sync.Cond
	wg          sync.WaitGroup
	timer       *time.Timer
	maxAttempts int
	requeues    int
	baseBackoff time.Duration
	done        chan struct{}
}

func newNode(tasks []*Job, workers []*Worker) *Node {
	n := &Node{
		tasks:       jobHeap(tasks),
		workers:     workers,
		maxAttempts: 3,
		baseBackoff: 50 * time.Millisecond,
		done:        make(chan struct{}),
	}
	heap.Init(&n.tasks)
	n.cond = sync.NewCond(&n.mu)
	return n
}

func (node *Node) pullJob() *Job {
	node.mu.Lock()
	defer node.mu.Unlock()

	for {
		select {
		case <-node.done:
			return nil
		default:
		}
		if len(node.tasks) > 0 && time.Now().After(node.tasks[0].readyAt) {
			return heap.Pop(&node.tasks).(*Job)
		}
		node.cond.Wait()
	}
}

// won't need lock, requeue job
func (node *Node) retryJob(worker *Worker, job *Job) {
	job.attempt++
	worker.currJob = nil

	if job.attempt >= node.maxAttempts {
		job.state = StateFailed
		node.deadTasks = append(node.deadTasks, job)
		node.wg.Done()
		return
	}

	// backoff, wait until requeuing job
	backoff := node.baseBackoff * time.Duration(1<<(job.attempt-1))
	job.readyAt = time.Now().Add(backoff)
	job.state = StatePending
	job.epoch++
	heap.Push(&node.tasks, job)
	node.cond.Signal()
}

func (node *Node) runWorker(worker *Worker) {
	for {
		currJob := node.pullJob()
		if currJob == nil {
			return
		}

		// epoch is used to stop jobs from being marked done more than once.
		// checks whether the epoch of job matches epoch of worker.
		// if not equal, then do not mark as done to prevent duplicates
		node.mu.Lock()
		currEpoch := currJob.epoch
		worker.lastBeat = time.Now()

		worker.currJob = currJob
		currJob.state = StateRunning
		node.mu.Unlock()

		duration := currJob.duration
		endTime := time.Now().Add(duration)

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
			if currJob.attempt < currJob.failCount {
				node.retryJob(worker, currJob)
			} else {
				currJob.state = StateDone
				worker.currJob = nil
				node.wg.Done()
			}
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
		select {
		case <-node.done:
			return
		default:
			node.mu.Lock()
			for i := range node.workers {
				lastBeat := node.workers[i].lastBeat
				if time.Since(lastBeat) > timeout && node.workers[i].currJob != nil {
					node.retryJob(node.workers[i], node.workers[i].currJob)
				}
			}
			node.cond.Broadcast()
			node.mu.Unlock()
		}
		time.Sleep(interval)
	}
}

func (node *Node) stop() {
	close(node.done)
	node.mu.Lock()
	node.cond.Broadcast()
	node.mu.Unlock()
}
