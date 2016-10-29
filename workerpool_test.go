package gpool

import (
	"sync"
	"testing"
)

func TestPoolCreate(t *testing.T) {
	wp, _ := NewLimit(10, 5)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		idx := i
		fn := func() {
			println("hello", idx)
			wg.Done()
		}
		if err := wp.SyncQueue(fn); err != nil {
			println(err.Error())
		}
	}
	wg.Wait()
}
