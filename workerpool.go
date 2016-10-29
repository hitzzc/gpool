package gpool

import (
	"errors"
	"sync"
	"time"
)

const (
	MaxJobNum int = 100 * 100
)

var (
	// need have a more suitable error name
	ErrMaxWorkerNum = errors.New("worker number excess max")
	ErrStopped      = errors.New("WorkerPool has been stopped")
)

// WorkerPool define a worker pool
type WorkerPool struct {
	sync.WaitGroup //for worker sync

	maxWorkerNumber     int           //the max worker number in the pool
	currentWorkerNumber int           //the worker number now in the pool
	maxIdleTime         time.Duration //the recycle time. That means goroutine will be destroyed when it has not been used for maxIdleTime.
	limited             bool          // unlimited or limited number worker pool

	jobQueue     chan *job
	pendingQueue chan chan func()
	stop         chan struct{} //stop

	objectPool *sync.Pool //gc-friendly
}

type job struct {
	fn       func()
	callback chan error
	syncFlag bool
}

// NewLimt creates a worker pool
//
// maxWorkerNum define the max worker number in the pool. When the worker
// number exceeds maxWorkerNum, we will ignore the job.
// recycleTime(minute) define the time to recycle goroutine. When a goroutine has
// not been used for recycleTime, it will be recycled.
func NewLimit(maxWorkerNum, recycleTime int) (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: maxWorkerNum,
		maxIdleTime:     time.Duration(recycleTime) * time.Minute,
		jobQueue:        make(chan *job, MaxJobNum),
		pendingQueue:    make(chan chan func(), maxWorkerNum),
		limited:         true,
	}
	go wp.supervisor()
	return wp, nil
}

// NewUnlimit creates a unlimited-number worker pool.
func NewUnlimit(recycleTime int) (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: -1,
		maxIdleTime:     time.Duration(recycleTime) * time.Minute,
		jobQueue:        make(chan *job, MaxJobNum),
	}

	go wp.supervisor()
	return wp, nil
}

// init initializes the workerpool.
//
// init func will be in charge of cleaning up goroutines and receiving the stop signal.
func (wp *WorkerPool) supervisor() {
L:
	for {
		select {
		case <-wp.stop:
			wp.Wait()
			break L
		case j := <-wp.jobQueue:
			if !wp.limited {
				go j.fn()
				if j.syncFlag {
					j.callback <- nil
				}
			} else {
				wp.handleJob(j)
			}
		}
	}
}

func (wp *WorkerPool) handleJob(j *job) {
	var err error
	defer func() {
		if j.syncFlag {
			j.callback <- err
		}
	}()
	for {
		select {
		case <-wp.stop:
			err = ErrStopped
			return
		case funcCh := <-wp.pendingQueue:
			funcCh <- j.fn
			return
		default:
			if wp.maxWorkerNumber != -1 && wp.currentWorkerNumber > wp.maxWorkerNumber {
				err = ErrMaxWorkerNum
				return
			}
			started := make(chan struct{}, 1)
			wp.Add(1)
			wp.currentWorkerNumber++
			go wp.worker(started)
			<-started
		}
	}
}

// stopPool stops the worker pool.
func (wp *WorkerPool) StopPool() {
	close(wp.stop)
}

// Queue assigns a worker for job (fn func(), with closure we can define every job in this form)
//
// If the worker pool is limited-number and the worker number has reached the limit, we prefer to discard the job.
func (wp *WorkerPool) SyncQueue(fn func()) (err error) {
	select {
	case <-wp.stop:
		return ErrStopped
	default:
	}
	j := &job{fn: fn, callback: make(chan error, 1), syncFlag: true}
	wp.jobQueue <- j
	err = <-j.callback
	return
}
func (wp *WorkerPool) AsyncQueue(fn func()) {
	select {
	case <-wp.stop:
		return
	default:
	}
	wp.jobQueue <- &job{fn: fn}
	return
}

// StartWorker starts a new goroutine.
func (wp *WorkerPool) worker(started chan<- struct{}) {
	first := true
	defer wp.Done()
	timer := time.NewTimer(wp.maxIdleTime)
	funcCh := make(chan func(), 1)
	for {
		wp.pendingQueue <- funcCh
		if first {
			started <- struct{}{}
			first = false
		}
		select {
		case <-wp.stop:
			return
		case <-timer.C:
			return
		case f := <-funcCh:
			if f == nil {
				continue
			}
			f()
		}
		timer.Reset(wp.maxIdleTime)
	}
}
