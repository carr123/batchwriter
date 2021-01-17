package batchwriter

import (
	"fmt"
	"sync"
	"time"
)

type AsyncBatchWriter struct {
	nCounter          uint64
	locker            sync.Mutex
	mp                map[string]interface{}
	inChan            chan interface{}
	nDesiredBatchSize int
	nMaxWait          time.Duration
	wg                sync.WaitGroup
	BatchWrite        func([]interface{})
}

func NewAsyncBatchWriter(nDesiredBatchSize int, buffsize int, maxWait time.Duration) *AsyncBatchWriter {
	obj := &AsyncBatchWriter{
		mp:                make(map[string]interface{}),
		inChan:            make(chan interface{}, buffsize),
		nDesiredBatchSize: nDesiredBatchSize,
		nMaxWait:          maxWait,
	}

	obj.wg.Add(1)
	go obj._performMerge()

	return obj
}

func (t *AsyncBatchWriter) PostMessage(msg interface{}) {
	t.locker.Lock()
	key := fmt.Sprintf("UNIQUE_KEY_%d", t.nCounter)
	t.mp[key] = msg
	t.nCounter++
	t.locker.Unlock()

	t.inChan <- key
}

func (t *AsyncBatchWriter) PostMessageUnique(key string, msg interface{}) {
	var bInsert bool = false
	t.locker.Lock()
	if _, ok := t.mp[key]; !ok {
		t.mp[key] = msg
		bInsert = true
	} else {
		t.mp[key] = msg
	}
	t.locker.Unlock()

	if bInsert {
		t.inChan <- key
	}
}

//make sure NO goroutine is calling PostMessage/PostMessageUnique before calling Close()
//otherwise sending data to a broken channel can cause panic
func (t *AsyncBatchWriter) Close() {
	close(t.inChan)
	t.wg.Wait()
}

func (t *AsyncBatchWriter) _performMerge() {
	defer t.wg.Done()

	keys := make([]string, 0, t.nDesiredBatchSize)
	values := make([]interface{}, 0, t.nDesiredBatchSize)

	doWrite := func() {
		if len(keys) == 0 {
			return
		}

		t.locker.Lock()
		for _, key := range keys {
			values = append(values, t.mp[key])
			delete(t.mp, key)
		}
		t.locker.Unlock()

		t.BatchWrite(values)
		keys = keys[:0]
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
		case item := <-t.inChan:
			if item == nil {
				bExit = true
			} else {
				keys = append(keys, item.(string))
			}
		}

		if bExit {
			doWrite()
			return
		}

		if (bNeedWrite && len(t.inChan) == 0 && len(keys) > 0) || len(keys) >= t.nDesiredBatchSize {
			doWrite()
			bNeedWrite = false
		}
	}
}

/*

最佳写入性能是调试最佳 nDesiredBatchSize, 不同环境的最佳写入batch不同
btWriter := batchwriter.NewAsyncBatchWriter(1000, time.Second*3)
btWriter.BatchWrite = func(arr []interface{}) {
	log.Println("recv batch:", len(arr))
}
btWriter.PostMessage("hello")

*/
