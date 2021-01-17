package batchwriter

import (
	"sync"
	"time"
)

type ElementT struct {
	data   interface{}
	notify chan error
}

type SyncBatchWriter struct {
	pool              sync.Pool
	inChan            chan *ElementT
	nDesiredBatchSize int
	nMaxWait          time.Duration
	wg                sync.WaitGroup
	BatchWrite        func([]interface{}) error
}

func NewSyncBatchWriter(nDesiredBatchSize int, buffsize int, maxWait time.Duration) *SyncBatchWriter {
	obj := &SyncBatchWriter{
		inChan:            make(chan *ElementT, buffsize),
		nDesiredBatchSize: nDesiredBatchSize,
		nMaxWait:          maxWait,
	}
	obj.pool.New = func() interface{} {
		el := &ElementT{
			notify: make(chan error, 1),
		}
		return el
	}

	obj.wg.Add(1)
	go obj._performMerge()

	return obj
}

func (t *SyncBatchWriter) SendMessage(msg interface{}) error {
	el := t.pool.Get().(*ElementT)
	el.data = msg
	for {
		if len(el.notify) == 0 {
			break
		}
		<-el.notify
	}
	t.inChan <- el
	err := <-el.notify
	el.data = nil
	t.pool.Put(el)

	return err
}

//make sure NO goroutine is calling SendMessage before calling Close()
//otherwise sending data to a broken channel can cause panic
func (t *SyncBatchWriter) Close() {
	close(t.inChan)
	t.wg.Wait()
}

func (t *SyncBatchWriter) _performMerge() {
	defer t.wg.Done()

	elements := make([]*ElementT, 0, t.nDesiredBatchSize)
	values := make([]interface{}, 0, t.nDesiredBatchSize)

	doWrite := func() {
		if len(elements) == 0 {
			return
		}

		for _, el := range elements {
			values = append(values, el.data)
		}

		err := t.BatchWrite(values)

		for _, el := range elements {
			el.notify <- err
		}

		elements = elements[:0]
		values = values[:0]
	}

	ticker := time.NewTicker(t.nMaxWait)
	defer ticker.Stop()

	var bExit bool = false
	var bNeedWrite bool = false

	for {
		select {
		case <-ticker.C:
			bNeedWrite = true
		case el := <-t.inChan:
			if el == nil {
				bExit = true
			} else {
				elements = append(elements, el)
			}
		}

		if bExit {
			doWrite()
			return
		}

		if (bNeedWrite && len(t.inChan) == 0 && len(elements) > 0) || len(elements) >= t.nDesiredBatchSize {
			doWrite()
			bNeedWrite = false
		}
	}
}

/*
最佳写入性能是调试最佳 nDesiredBatchSize, 不同环境的最佳写入batch不同
w := batchwriter.NewsyncBatchWriter(10, time.Millisecond*5)
w.BatchWrite = func(items []interface{}) error{
	//log.Println("recv:", items)
	time.Sleep(time.Millisecond * 10)
	return nil
}

var wg sync.WaitGroup

t1 := time.Now()
for i := 0; i < 14; i++ {
	wg.Add(1)
	go func(n int) {
		w.SendMessage(fmt.Sprintf("data%d", n))
		fmt.Printf("item%d time taken:%v\n", n, time.Now().Sub(t1))
	}(i)
}

time.Sleep(time.Millisecond * 10)
w.Close()
wg.Wait()

*/
