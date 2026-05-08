package queue

import "sync"

type fastQueue[T any] struct {
	head          *queueItem[T]
	tail          *queueItem[T]
	cond          *sync.Cond
	lock          *sync.Mutex
	deBlock       bool
	ignoreEnqueue bool
}

// NewFastQueue creates a new instance of a linked list implementer of the Queue interface
// where only non-read-only operations are synchronised and unreachable data is not cleared directly
func NewFastQueue[T any]() Queue[T] {
	lock := &sync.Mutex{}
	return &fastQueue[T]{
		head: nil,
		tail: nil,
		cond: sync.NewCond(lock),
		lock: lock,
	}
}

// Enqueue a value of type T
func (q *fastQueue[T]) Enqueue(value T) {
	if q.ignoreEnqueue {
		return
	}
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.head == nil {
		q.head = &queueItem[T]{value: value}
		q.tail = q.head
	} else {
		q.tail.next = &queueItem[T]{value: value}
		q.tail = q.tail.next
	}
	q.cond.Signal()
}

// Pop a value of a pointer of type T blocking for a value when IsEmpty
// (nil when nothing to dequeue and IsUnBlocking is false)
func (q *fastQueue[T]) Pop() *T {
	q.lock.Lock()
	defer q.lock.Unlock()
	for q.head == nil && !q.deBlock {
		q.cond.Wait()
	}
	if q.head == nil {
		return nil
	}
	value := q.head.value
	if q.head == q.tail {
		q.tail = nil
	}
	q.head = q.head.next
	return &value
}

// Dequeue a value of type T blocking for a value when IsEmpty
// (default value of type T when nothing to dequeue and IsUnBlocking is false)
func (q *fastQueue[T]) Dequeue() T {
	var value T
	pval := q.Pop()
	if pval != nil {
		value = *pval
	}
	return value
}

// IsEmpty of items in queue
func (q *fastQueue[T]) IsEmpty() bool {
	return q.head == nil
}

// Peek first queue value (nil when empty)
func (q *fastQueue[T]) Peek() *T {
	if q.head == nil {
		return nil
	}
	return &q.head.value
}

// PeekLast queue value (nil when empty)
func (q *fastQueue[T]) PeekLast() *T {
	if q.tail == nil {
		return nil
	}
	return &q.tail.value
}

// StartUnBlocking the queue allowing Dequeue and Pop to return when no items are present and sets IsBlockingEnqueue
func (q *fastQueue[T]) StartUnBlocking() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.deBlock = true
	q.ignoreEnqueue = true
	q.cond.Broadcast()
}

// EndUnBlocking the queue restores normal operations and unsets IsBlockingEnqueue
func (q *fastQueue[T]) EndUnBlocking() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.deBlock = false
	q.ignoreEnqueue = false
}

// IsUnBlocking the queue, whether Dequeue and Pop return when no items are present
func (q *fastQueue[T]) IsUnBlocking() bool {
	return q.deBlock
}

// BlockEnqueue operations
func (q *fastQueue[T]) BlockEnqueue() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.ignoreEnqueue = true
}

// UnBlockEnqueue operations
func (q *fastQueue[T]) UnBlockEnqueue() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.ignoreEnqueue = false
}

// IsBlockingEnqueue operations
func (q *fastQueue[T]) IsBlockingEnqueue() bool {
	return q.ignoreEnqueue
}

// Clear the queue
func (q *fastQueue[T]) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.head = nil
	q.tail = nil
	q.cond.Broadcast()
}
