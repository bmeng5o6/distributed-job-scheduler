package scheduler

import (
	"testing"
	"time"
)

func TestScheduler_BasicSchedule(t *testing.T) {
	jobList, workerList := []*Job{}, []*Worker{}

	jobList = append(jobList, newJob("task one", 100*time.Millisecond))
	jobList = append(jobList, newJob("task two", 100*time.Millisecond))
	jobList = append(jobList, newJob("task three", 100*time.Millisecond))

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

func TestSCheduler_WorkerDeath(t *testing.T) {
	jobList, workerList := []*Job{}, []*Worker{}

	jobList = append(jobList, newJob("task one", 100*time.Millisecond))
	jobList = append(jobList, newJob("task two", 100*time.Millisecond))
	jobList = append(jobList, newJob("task three", 100*time.Millisecond))

	workerList = append(workerList, newWorker("worker one"))
	workerList = append(workerList, newWorker("worker two"))
	workerList = append(workerList, newWorker("worker three"))

	currNode := newNode(jobList, workerList)
	currNode.wg.Add(len(jobList))
	currNode.start()

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
