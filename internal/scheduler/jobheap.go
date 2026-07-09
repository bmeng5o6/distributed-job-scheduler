package scheduler

type jobHeap []*Job

func (h jobHeap) Len() int {
	return len(h)
}

func (h jobHeap) Less(i, j int) bool {
	return h[i].readyAt.Before(h[j].readyAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *jobHeap) Push(x any) {
	*h = append(*h, x.(*Job))
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	job := old[n-1]
	*h = old[:n-1]
	return job
}
