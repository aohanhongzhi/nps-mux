package nps_mux

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestPopContention(t *testing.T) {
	log.Print("开始执行")
	t.Log("开始")
	q := newReceiveWindowQueue()
	q.SetTimeOut(time.Now().Add(time.Second * 5))
	var wg sync.WaitGroup

	// 启动 100 个消费者
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {

			defer wg.Done()
			for j := 0; j < 1; j++ {
				_, _ = q.Pop()
			}
		}()
	}

	// 生产者填充数据
	for i := 0; i < 10; i++ {
		q.Push(&listElement{Buf: make([]byte, 10), L: 10})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Second):
		t.Fatal("Test timed out")
	}
	log.Print("执行完成了")
}
