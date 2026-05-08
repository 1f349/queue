package queue

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
