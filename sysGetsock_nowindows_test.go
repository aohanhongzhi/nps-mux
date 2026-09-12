//go:build !windows
// +build !windows

package nps_mux

import (
	"net"
	"sync"
	"testing"
)

func TestSysGetSockConcurrentClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		fd, err := socket.File()
		if err != nil {
			socket.Close()
			t.Fatal(err)
		}
		if size, err := sysGetSock(fd); err != nil || size <= 0 {
			fd.Close()
			socket.Close()
			t.Fatalf("invalid buffer size %d: %v", size, err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				sysGetSock(fd)
			}
		}()
		go func() { defer wg.Done(); <-start; fd.Close() }()
		close(start)
		wg.Wait()
		if _, err := sysGetSock(fd); err == nil {
			t.Fatal("query of closed descriptor succeeded")
		}
		socket.Close()
	}
}
