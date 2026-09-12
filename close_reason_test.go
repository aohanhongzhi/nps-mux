package nps_mux

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestCloseErrorKind(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, "none"},
		{fmt.Errorf("read: %w", io.EOF), "eof"},
		{&net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, "tcp_reset"},
		{&net.DNSError{IsTimeout: true}, "timeout"},
		{io.ErrUnexpectedEOF, "other"},
	} {
		if got := closeErrorKind(tc.err); got != tc.want {
			t.Errorf("%v: got %q, want %q", tc.err, got, tc.want)
		}
	}
}
