package queue

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testQueue[T comparable](t *testing.T, t_vals []T, initial T, q Queue[T]) {
	t.Run("Sync", func(t *testing.T) {
		testQueueSync(t, q, t_vals, initial)
	})
	t.Run("Async", func(t *testing.T) {
		testQueueAsync(t, q, t_vals, initial)
	})
}

func testQueueAsync[T comparable](t *testing.T, q Queue[T], t_vals []T, initial T) {
	t.Run("InitialState", func(t *testing.T) {
		assert.GreaterOrEqual(t, len(t_vals), 4)
		assert.True(t, q.IsEmpty())
		assert.False(t, q.IsBlockingEnqueue())
		assert.False(t, q.IsUnBlocking())
	})
	var exit chan struct{}
	var texit chan struct{}
	var waiter *sync.WaitGroup
	var wait *time.Timer
	var blocked = false
	setup := func(x int) {
		q.EndUnBlocking()
		assert.False(t, q.IsUnBlocking())
		exit = make(chan struct{})
		waiter = &sync.WaitGroup{}
		waiter.Add(x)
	}
	tearDown := func() {
		wait = time.NewTimer(time.Second)
		texit = make(chan struct{})
		go func() {
			defer wait.Stop()
			select {
			case <-exit:
				blocked = false
			case <-wait.C:
				blocked = true
			}
			q.StartUnBlocking()
			close(texit)
		}()
		waiter.Wait()
		close(exit)
		<-texit
	}
	setEmpty := func() {
		q.Clear()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
	}
	t.Run("Enqueue4ValueDequeue3Value", func(t *testing.T) {
		setup(7)
		for i := 0; i < 4; i++ {
			go func() {
				q.Enqueue(t_vals[i])
				waiter.Done()
			}()
		}
		for i := 0; i < 3; i++ {
			go func() {
				dequeued := q.Dequeue()
				assert.Contains(t, t_vals, dequeued)
				waiter.Done()
			}()
		}
		tearDown()
		assert.False(t, q.IsEmpty())
		assert.Equal(t, q.Peek(), q.PeekLast())
		assert.False(t, blocked)
		setEmpty()
	})
	t.Run("Enqueue3ValueDequeue4Value", func(t *testing.T) {
		cnt := &atomic.Int32{}
		cnt.Store(0)
		setup(7)
		for i := 0; i < 3; i++ {
			go func() {
				q.Enqueue(t_vals[i])
				waiter.Done()
			}()
		}
		for i := 0; i < 4; i++ {
			go func() {
				dequeued := q.Dequeue()
				assert.Contains(t, append(t_vals, initial), dequeued)
				if dequeued != initial {
					cnt.Add(1)
				}
				waiter.Done()
			}()
		}
		tearDown()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Equal(t, q.Peek(), q.PeekLast())
		assert.True(t, blocked)
		assert.Equal(t, int32(3), cnt.Load())
	})
	t.Run("Enqueue4ValueClearPop1Value", func(t *testing.T) {
		cnt := &atomic.Int32{}
		cnt.Store(0)
		setup(6)
		for i := 0; i < 4; i++ {
			go func() {
				q.Enqueue(t_vals[i])
				waiter.Done()
			}()
		}
		go func() {
			q.Clear()
			waiter.Done()
		}()
		go func() {
			popped := q.Pop()
			if popped != nil {
				assert.Contains(t, t_vals, *popped)
				cnt.Add(1)
			}
			waiter.Done()
		}()
		tearDown()
		popped := q.Pop()
		for popped != nil {
			assert.Contains(t, t_vals, *popped)
			popped = q.Pop()
			cnt.Add(1)
		}
		assert.LessOrEqual(t, cnt.Load(), int32(4))
	})
	t.Run("Enqueue4ValuePop3Value", func(t *testing.T) {
		setup(7)
		for i := 0; i < 4; i++ {
			go func() {
				q.Enqueue(t_vals[i])
				waiter.Done()
			}()
		}
		for i := 0; i < 3; i++ {
			go func() {
				popped := q.Pop()
				assert.NotNil(t, popped)
				assert.Contains(t, t_vals, *popped)
				waiter.Done()
			}()
		}
		tearDown()
		assert.False(t, q.IsEmpty())
		assert.Equal(t, q.Peek(), q.PeekLast())
		assert.False(t, blocked)
		setEmpty()
	})
	t.Run("Enqueue3ValuePop4Value", func(t *testing.T) {
		cnt := &atomic.Int32{}
		cnt.Store(0)
		setup(7)
		for i := 0; i < 3; i++ {
			go func() {
				q.Enqueue(t_vals[i])
				waiter.Done()
			}()
		}
		for i := 0; i < 4; i++ {
			go func() {
				popped := q.Pop()
				if popped != nil {
					cnt.Add(1)
					assert.Contains(t, t_vals, *popped)
				}
				waiter.Done()
			}()
		}
		tearDown()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Equal(t, q.Peek(), q.PeekLast())
		assert.True(t, blocked)
		assert.Equal(t, int32(3), cnt.Load())
	})
}

func testQueueSync[T comparable](t *testing.T, q Queue[T], t_vals []T, initial T) {
	t.Run("InitialState", func(t *testing.T) {
		assert.GreaterOrEqual(t, len(t_vals), 3)
		assert.True(t, q.IsEmpty())
		assert.False(t, q.IsBlockingEnqueue())
		assert.False(t, q.IsUnBlocking())
	})
	t.Run("PeekEmpty", func(t *testing.T) {
		assert.Nil(t, q.Peek())
	})
	t.Run("PeekLastEmpty", func(t *testing.T) {
		assert.Nil(t, q.PeekLast())
	})
	t.Run("IsEmpty", func(t *testing.T) {
		assert.True(t, q.IsEmpty())
	})
	t.Run("PeekPeekLast1Value", func(t *testing.T) {
		q.Enqueue(t_vals[0])
		assert.False(t, q.IsEmpty())
		assert.NotNil(t, q.Peek())
		assert.NotNil(t, q.PeekLast())
		assert.Equal(t, q.Peek(), q.PeekLast())
		assert.Equal(t, t_vals[0], *q.Peek())
	})
	t.Run("IsEmpty1Value", func(t *testing.T) {
		assert.False(t, q.IsEmpty())
	})
	t.Run("PeekPeekLast2Value", func(t *testing.T) {
		q.Enqueue(t_vals[1])
		assert.False(t, q.IsEmpty())
		assert.NotNil(t, q.Peek())
		assert.NotNil(t, q.PeekLast())
		assert.Equal(t, t_vals[0], *q.Peek())
		assert.Equal(t, t_vals[1], *q.PeekLast())
	})
	t.Run("IsEmpty2Value", func(t *testing.T) {
		assert.False(t, q.IsEmpty())
	})
	t.Run("Pop", func(t *testing.T) {
		assert.False(t, q.IsEmpty())
		peeked := q.Peek()
		assert.NotNil(t, peeked)
		popped := q.Pop()
		assert.NotNil(t, popped)
		assert.Equal(t, *peeked, *popped)
		assert.Equal(t, t_vals[0], *popped)
		assert.False(t, q.IsEmpty())
		assert.Equal(t, *q.Peek(), *q.PeekLast())
		assert.Equal(t, t_vals[1], *q.Peek())
	})
	t.Run("Dequeue", func(t *testing.T) {
		assert.False(t, q.IsEmpty())
		dequeued := q.Dequeue()
		assert.Equal(t, t_vals[1], dequeued)
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
	})
	t.Run("PopNilEmpty", func(t *testing.T) {
		q.StartUnBlocking()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.True(t, q.IsUnBlocking())
		assert.True(t, q.IsBlockingEnqueue())
		popped := q.Pop()
		assert.Nil(t, popped)
		q.EndUnBlocking()
		assert.False(t, q.IsUnBlocking())
		assert.False(t, q.IsBlockingEnqueue())
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
	})
	t.Run("DequeueInitialEmpty", func(t *testing.T) {
		q.StartUnBlocking()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.True(t, q.IsUnBlocking())
		assert.True(t, q.IsBlockingEnqueue())
		dequeued := q.Dequeue()
		assert.Equal(t, initial, dequeued)
		q.EndUnBlocking()
		assert.False(t, q.IsUnBlocking())
		assert.False(t, q.IsBlockingEnqueue())
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
	})
	t.Run("Clear3Value", func(t *testing.T) {
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
		q.Enqueue(t_vals[0])
		q.Enqueue(t_vals[1])
		q.Enqueue(t_vals[2])
		assert.False(t, q.IsEmpty())
		assert.Equal(t, t_vals[0], *q.Peek())
		assert.Equal(t, t_vals[2], *q.PeekLast())
		q.Clear()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
	})
	t.Run("BlockEnqueue", func(t *testing.T) {
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
		q.Enqueue(t_vals[0])
		q.BlockEnqueue()
		assert.False(t, q.IsEmpty())
		assert.Equal(t, t_vals[0], *q.Peek())
		assert.True(t, q.IsBlockingEnqueue())
		assert.False(t, q.IsUnBlocking())
		q.Enqueue(t_vals[1])
		assert.Equal(t, t_vals[0], *q.PeekLast())
		assert.NotEqual(t, t_vals[1], *q.PeekLast())
		assert.Equal(t, t_vals[0], q.Dequeue())
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
		q.Enqueue(t_vals[2])
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
		q.UnBlockEnqueue()
		assert.False(t, q.IsBlockingEnqueue())
		q.Enqueue(t_vals[0])
		assert.False(t, q.IsEmpty())
		assert.Equal(t, t_vals[0], *q.Peek())
		q.Clear()
		assert.True(t, q.IsEmpty())
		assert.Nil(t, q.Peek())
		assert.Nil(t, q.PeekLast())
	})
}

func TestQueue(t *testing.T) {
	var x int
	testQueue[int](t, []int{1, 2, 3, 4, 5, 6, 7, 8}, x, NewQueue[int]())
}

func TestQueuePointable(t *testing.T) {
	var x *int
	t_vals := []int{1, 2, 3, 4, 5, 6, 7, 8}
	p_t_vals := []*int{}
	for _, v := range t_vals {
		p_t_vals = append(p_t_vals, &v)
	}
	testQueue[*int](t, p_t_vals, x, NewQueue[*int]())
}

func TestFastQueue(t *testing.T) {
	var x int
	testQueue[int](t, []int{1, 2, 3, 4, 5, 6, 7, 8}, x, NewFastQueue[int]())
}

func TestFastQueuePointable(t *testing.T) {
	var x *int
	t_vals := []int{1, 2, 3, 4, 5, 6, 7, 8}
	p_t_vals := []*int{}
	for _, v := range t_vals {
		p_t_vals = append(p_t_vals, &v)
	}
	testQueue[*int](t, p_t_vals, x, NewFastQueue[*int]())
}
