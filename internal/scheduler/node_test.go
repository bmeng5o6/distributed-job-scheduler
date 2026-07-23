package scheduler

import (
	"testing"
	"time"
)

func TestScheduler_BasicSchedule(t *testing.T) {
	jobList, workerList := []*Job{}, []*Worker{}

	jobList = append(jobList, newJob("task one", 100*time.Millisecond, 1))
	jobList = append(jobList, newJob("task two", 100*time.Millisecond, 1))
	jobList = append(jobList, newJob("task three", 100*time.Millisecond, 1))

	workerList = append(workerList, newWorker("worker one"))
	workerList = append(workerList, newWorker("worker two"))
	workerList = append(workerList, newWorker("worker three"))

	currNode := newNode(jobList, workerList)
	currNode.wg.Add(len(jobList))
	currNode.start()
	currNode.wg.Wait()

	for i := range workerList {
		worker := workerList[i]
		if worker.currJob != nil {
			t.Errorf("expected workers all free, got %s", "false")
		}
	}

	for i := range jobList {
		if jobList[i].state != StateDone {
			t.Errorf("job %d: expected done, got %s", i, jobList[i].state)
		}
	}

	if len(currNode.tasks) != 0 {
		t.Errorf("expected jobList empty and workerList all free, did not get")
	}

}

func TestScheduler_WorkerDeath(t *testing.T) {
	jobList, workerList := []*Job{}, []*Worker{}

	jobList = append(jobList, newJob("task one", 100*time.Millisecond, 1))
	jobList = append(jobList, newJob("task two", 100*time.Millisecond, 1))
	jobList = append(jobList, newJob("task three", 100*time.Millisecond, 1))

	workerList = append(workerList, newWorker("worker one"))
	workerList = append(workerList, newWorker("worker two"))
	workerList = append(workerList, newWorker("worker three"))

	currNode := newNode(jobList, workerList)
	currNode.wg.Add(len(jobList))
	currNode.start()
	defer currNode.stop()

	time.Sleep(30 * time.Millisecond)
	currNode.mu.Lock()
	workerList[0].alive = false
	currNode.mu.Unlock()

	currNode.wg.Wait()

	for i := range workerList {
		worker := workerList[i]
		if worker.currJob != nil {
			t.Errorf("expected workers all free, got %s", "false")
		}
	}

	for i := range jobList {
		if jobList[i].state != StateDone {
			t.Errorf("job %d: expected done, got %s", i, jobList[i].state)
		}
	}

	if len(currNode.tasks) != 0 {
		t.Errorf("expected jobList empty and workerList all free, did not get")
	}

}

func TestScheduler_RetryAndDeadLetter(t *testing.T) {
	jobList, workerList := []*Job{}, []*Worker{}

	retryJob := newJob("retry", 100*time.Millisecond, 2)
	failedJob := newJob("fail", 100*time.Millisecond, 10)

	jobList = append(jobList, retryJob)
	jobList = append(jobList, failedJob)

	workerList = append(workerList, newWorker("worker one"))
	workerList = append(workerList, newWorker("worker two"))

	currNode := newNode(jobList, workerList)
	currNode.wg.Add(len(jobList))
	currNode.start()
	defer currNode.stop()
	currNode.wg.Wait()

	if retryJob.state != StateDone {
		t.Errorf("Retry job: expected done, got %s", retryJob.state)
	}

	if failedJob.state != StateFailed {
		t.Errorf("Fail job: expected fail, got %s", retryJob.state)
	}

	if len(currNode.deadTasks) != 1 {
		t.Errorf("Expected one dead task, got %d", len(currNode.deadTasks))
	}
}

func TestScheduler_DeathDoesNotUseRetryBudget(t *testing.T) {
	job := newJob("stay", 50*time.Millisecond, 0) // would succeed first try
	workers := []*Worker{newWorker("w1"), newWorker("w2")}

	n := newNode([]*Job{job}, workers)
	n.wg.Add(1)
	n.start()
	defer n.stop()

	// kill whichever worker grabs it
	time.Sleep(20 * time.Millisecond)
	n.mu.Lock()
	for _, w := range workers {
		if w.currJob != nil {
			w.alive = false
			break
		}
	}
	n.mu.Unlock()

	n.wg.Wait()

	if job.state != StateDone {
		t.Errorf("expected done, got %s", job.state)
	}
	if job.attempt != 0 {
		t.Errorf("death should not bump attempt, got %d", job.attempt)
	}
	if job.requeues == 0 {
		t.Error("expected requeues to be bumped by death")
	}
}
