package queue

import "sync"

type queueItem[T any] struct {
	value T
	next  *queueItem[T]
}

// Queue provides a queue interface of type T
type Queue[T any] interface {
	// Enqueue a value of type T
	Enqueue(T)
	// Dequeue a value of type T blocking for a value when IsEmpty
	// (default value of type T when nothing to dequeue and IsUnBlocking is false)
	Dequeue() T
	// Pop a value of a pointer of type T blocking for a value when IsEmpty
	// (nil when nothing to dequeue and IsUnBlocking is false)
	Pop() *T
	// Peek first queue value (nil when empty)
	Peek() *T
	// PeekLast queue value (nil when empty)
	PeekLast() *T
	// IsEmpty of items in queue
	IsEmpty() bool
	// Clear the queue
	Clear()
	// StartUnBlocking the queue allowing Dequeue and Pop to return when no items are present and sets IsBlockingEnqueue
	StartUnBlocking()
	// EndUnBlocking the queue restores normal operations and unsets IsBlockingEnqueue
	EndUnBlocking()
	// BlockEnqueue operations
	BlockEnqueue()
	// UnBlockEnqueue operations
	UnBlockEnqueue()
	// IsUnBlocking the queue, whether Dequeue and Pop return when no items are present
	IsUnBlocking() bool
	// IsBlockingEnqueue operations
	IsBlockingEnqueue() bool
}

type queue[T any] struct {
	head          *queueItem[T]
	tail          *queueItem[T]
	cond          *sync.Cond
	lock          *sync.RWMutex
	deBlock       bool
	ignoreEnqueue bool
}

// NewQueue creates a new instance of a linked list implementer of the Queue interface
func NewQueue[T any]() Queue[T] {
	lock := &sync.RWMutex{}
	return &queue[T]{
		head: nil,
		tail: nil,
		cond: sync.NewCond(lock),
		lock: lock,
	}
}

// Enqueue a value of type T
func (q *queue[T]) Enqueue(value T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.ignoreEnqueue {
		return
	}
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
func (q *queue[T]) Pop() *T {
	q.lock.Lock()
	defer q.lock.Unlock()
	for q.head == nil && !q.deBlock {
		q.cond.Wait()
	}
	var value T
	if q.head == nil {
		return nil
	}
	var dValue = value
	value = q.head.value
	if q.head == q.tail {
		q.tail = nil
	}
	oHead := q.head
	q.head = q.head.next
	oHead.value = dValue
	oHead.next = nil
	return &value
}

// Dequeue a value of type T blocking for a value when IsEmpty
// (default value of type T when nothing to dequeue and IsUnBlocking is false)
func (q *queue[T]) Dequeue() T {
	var value T
	pval := q.Pop()
	if pval != nil {
		value = *pval
	}
	return value
}

// IsEmpty of items in queue
func (q *queue[T]) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.head == nil
}

// Peek first queue value (nil when empty)
func (q *queue[T]) Peek() *T {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if q.head == nil {
		return nil
	}
	return &q.head.value
}

// PeekLast queue value (nil when empty)
func (q *queue[T]) PeekLast() *T {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if q.tail == nil {
		return nil
	}
	return &q.tail.value
}

// StartUnBlocking the queue allowing Dequeue and Pop to return when no items are present and sets IsBlockingEnqueue
func (q *queue[T]) StartUnBlocking() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.deBlock = true
	q.ignoreEnqueue = true
	q.cond.Broadcast()
}

// EndUnBlocking the queue restores normal operations and unsets IsBlockingEnqueue
func (q *queue[T]) EndUnBlocking() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.deBlock = false
	q.ignoreEnqueue = false
}

// IsUnBlocking the queue, whether Dequeue and Pop return when no items are present
func (q *queue[T]) IsUnBlocking() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.deBlock
}

// BlockEnqueue operations
func (q *queue[T]) BlockEnqueue() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.ignoreEnqueue = true
}

// UnBlockEnqueue operations
func (q *queue[T]) UnBlockEnqueue() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.ignoreEnqueue = false
}

// IsBlockingEnqueue operations
func (q *queue[T]) IsBlockingEnqueue() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.ignoreEnqueue
}

// Clear the queue
func (q *queue[T]) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	var dValue T
	cHead := q.head
	for cHead != nil {
		oHead := cHead
		cHead = oHead.next
		oHead.value = dValue
		oHead.next = nil
	}
	q.cond.Broadcast()
}
