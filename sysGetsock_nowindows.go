//go:build !windows
// +build !windows

package nps_mux

import (
	"net"
	"os"
	"syscall"
)

func sysGetSock(fd *os.File) (bufferSize int, err error) {
	if fd == nil {
		return 5 * 1024 * 1024, nil
	}
	raw, err := fd.SyscallConn()
	if err != nil {
		return 0, err
	}
	// Control holds a descriptor reference until getsockopt returns, preventing
	// concurrent Close from destroying or recycling the descriptor mid-query.
	var socketErr error
	err = raw.Control(func(socket uintptr) {
		bufferSize, socketErr = syscall.GetsockoptInt(int(socket), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	if err != nil {
		return 0, err
	}
	return bufferSize, socketErr
}

func getConnFd(c net.Conn) (fd *os.File, err error) {
	switch c.(type) {
	case *net.TCPConn:
		fd, err = c.(*net.TCPConn).File()
		if err != nil {
			return
		}
		return
	case *net.UDPConn:
		fd, err = c.(*net.UDPConn).File()
		if err != nil {
			return
		}
		return
	default:
		return
	}
}
