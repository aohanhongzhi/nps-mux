package nps_mux

import (
	"context"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	muxPingFlag uint8 = iota
	muxNewConnOk
	muxNewConnFail
	muxNewMsg
	muxNewMsgPart
	muxMsgSendOk
	muxNewConn
	muxConnClose
	muxPingReturn
	muxPing            int32 = -1
	maximumSegmentSize       = poolSizeWindow
	maximumWindowSize        = 1 << 27 // 1<<31-1 TCP slide window size is very large,
	// we use 128M, reduce memory usage
)

// 实现了 net.Listener 接口
type Mux struct {
	// 8-byte aligned atomic fields first
	latency uint64          // 8 bytes (atomic)
	connMap *connMap        // 8 bytes
	counter *latencyCounter // 8 bytes
	bw      *bandwidth      // 8 bytes

	// 8-byte channels
	newConnCh chan *conn    // 8 bytes
	closeChan chan struct{} // 8 bytes
	pingCh    chan []byte   // 8 bytes

	// 16-byte interfaces
	net.Listener          // 16 bytes
	conn         net.Conn // 16 bytes

	// 16-byte string
	connType string // 16 bytes

	// 4-byte fields packed with padding
	pingCheckTime      uint32  // 4 bytes (atomic)
	pingCheckThreshold uint32  // 4 bytes (atomic)
	id                 int32   // 4 bytes (atomic)
	IsClose            int32   // 4 bytes (atomic)
	_                  [4]byte // explicit padding

	// Final struct fields
	writeQueue   priorityQueue
	newConnQueue connQueue
}

func NewMux(c net.Conn, connType string, pingCheckThreshold int) *Mux {
	defer PanicHandler()
	//c.(*net.TCPConn).SetReadBuffer(0)
	//c.(*net.TCPConn).SetWriteBuffer(0)
	fd, err := getConnFd(c)
	if err != nil {
		log.Println(err)
	}
	var checkThreshold uint32
	if pingCheckThreshold <= 0 {
		if connType == "kcp" {
			checkThreshold = 20
		} else {
			checkThreshold = 60
		}
	} else {
		checkThreshold = uint32(pingCheckThreshold)
	}
	m := &Mux{
		conn:               c,
		connMap:            NewConnMap(),
		id:                 0,
		closeChan:          make(chan struct{}, 1),
		newConnCh:          make(chan *conn),
		bw:                 NewBandwidth(fd),
		IsClose:            0,
		connType:           connType,
		pingCh:             make(chan []byte),
		pingCheckThreshold: checkThreshold,
		counter:            newLatencyCounter(),
	}
	m.writeQueue.New()
	m.newConnQueue.New()
	//read session by flag
	m.readSession()
	//ping
	m.ping()
	m.writeSession()
	return m
}

func (s *Mux) NewConn() (*conn, error) {
	defer PanicHandler()
	if atomic.LoadInt32(&s.IsClose) != 0 {
		return nil, errors.New("the mux has closed")
	}
	conn := NewConn(s.getId(), s)
	//it must be Set before send
	s.connMap.Set(conn.connId, conn)
	s.sendInfo(muxNewConn, conn.connId, nil)
	//Set a timer timeout 120 second
	timer := time.NewTimer(time.Minute * 2)
	defer timer.Stop()
	select {
	case <-conn.connStatusOkCh:
		return conn, nil
	case <-timer.C:
	}
	return nil, errors.New("create connection fail，the server refused the connection")
}

func (s *Mux) Accept() (net.Conn, error) {
	defer PanicHandler()
	if atomic.LoadInt32(&s.IsClose) != 0 {
		return nil, errors.New("accpet error,the mux has closed")
	}
	conn := <-s.newConnCh
	if conn == nil {
		return nil, errors.New("accpet error,the conn has closed")
	}
	return conn, nil
}

func (s *Mux) Addr() net.Addr {
	defer PanicHandler()
	return s.conn.LocalAddr()
}

func (s *Mux) sendInfo(flag uint8, id int32, data interface{}) {
	defer PanicHandler()
	if atomic.LoadInt32(&s.IsClose) != 0 {
		return
	}
	var err error
	pack := muxPack.Get()
	err = pack.Set(flag, id, data)
	if err != nil {
		muxPack.Put(pack)
		log.Println("mux: New Pack err", err)
		_ = s.Close()
		return
	}
	s.writeQueue.Push(pack)
	return
}

func (s *Mux) writeSession() {
	defer PanicHandler()
	go func() {
		defer PanicHandler()
		for {
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			pack := s.writeQueue.Pop()
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			//if pack.flag == muxNewMsg || pack.flag == muxNewMsgPart {
			//	if pack.length >= 100 {
			//		log.Println("write session id", pack.id, "\n", string(pack.content[:100]))
			//	} else {
			//		log.Println("write session id", pack.id, "\n", string(pack.content[:pack.length]))
			//	}
			//}
			err := pack.Pack(s.conn)
			muxPack.Put(pack)
			if err != nil {
				log.Println("mux: Pack err", err)
				_ = s.Close()
				break
			}
		}
	}()
}

func (s *Mux) ping() {
	defer PanicHandler()
	go func() {
		defer PanicHandler()
		now, _ := time.Now().UTC().MarshalText()
		s.sendInfo(muxPingFlag, muxPing, now)
		// send the ping flag and Get the latency first
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()
		for {
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			select {
			case <-ticker.C:
			}
			if atomic.LoadUint32(&s.pingCheckTime) > s.pingCheckThreshold {
				log.Println("mux: ping time out, checktime", s.pingCheckTime, "threshold", s.pingCheckThreshold)
				_ = s.Close()
				// more than limit times not receive the ping return package,
				// mux conn is damaged, maybe a packet drop, close it
				break
			}
			now, _ = time.Now().UTC().MarshalText()
			s.sendInfo(muxPingFlag, muxPing, now)
			atomic.AddUint32(&s.pingCheckTime, 1)
		}
		return
	}()

	go func() {
		defer PanicHandler()
		var now time.Time
		var data []byte
		for {
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			select {
			case data = <-s.pingCh:
				atomic.StoreUint32(&s.pingCheckTime, 0)
			case <-s.closeChan:
				break
			}
			_ = now.UnmarshalText(data)
			latency := time.Now().UTC().Sub(now).Seconds()
			if latency > 0 {
				atomic.StoreUint64(&s.latency, math.Float64bits(s.counter.Latency(latency)))
				// convert float64 to bits, store it atomic
				//log.Println("ping", math.Float64frombits(atomic.LoadUint64(&s.latency)))
			}
			if cap(data) > 0 && atomic.LoadInt32(&s.IsClose) == 0 {
				windowBuff.Put(data)
			}
		}
	}()
}

func (s *Mux) readSession() {
	defer PanicHandler()
	go func() {
		defer PanicHandler()
		var connection *conn
		for {
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			connection = s.newConnQueue.Pop()
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break // make sure that is closed
			}
			s.connMap.Set(connection.connId, connection) //it has been Set before send ok
			s.newConnCh <- connection
			s.sendInfo(muxNewConnOk, connection.connId, nil)
		}
	}()
	go func() {
		defer PanicHandler()
		var pack *muxPackager
		var l uint16
		var err error

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerPool := make(chan struct{}, runtime.NumCPU()*2)

		for {
			select {
			case <-ctx.Done():
				return
			case workerPool <- struct{}{}:
				defer func() { <-workerPool }()
			}

			if atomic.LoadInt32(&s.IsClose) != 0 {
				return
			}

			pack = muxPack.Get()

			if err = s.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
				log.Println("mux: set read deadline err", err)
				_ = s.Close()
				break
			}

			s.bw.StartRead()
			if l, err = pack.UnPack(s.conn); err != nil {
				log.Println("mux: read session unpack from connection err", err)
				_ = s.Close()
				break
			}

			s.bw.SetCopySize(l)

			switch pack.flag {
			case muxNewConn:
				connection := NewConn(pack.id, s)
				go func(c *conn) {
					select {
					case <-ctx.Done():
						c.Close()
					default:
						s.newConnQueue.Push(c)
					}
				}(connection)
			case muxPingFlag:
				go func() {
					s.sendInfo(muxPingReturn, muxPing, pack.content)
					windowBuff.Put(pack.content)
				}()
			case muxPingReturn:
				select {
				case s.pingCh <- pack.content:
				default:
					windowBuff.Put(pack.content)
				}
			}
			if connection, ok := s.connMap.Get(pack.id); ok && atomic.LoadInt32(&connection.isClose) == 0 {
				switch pack.flag {
				case muxNewMsg, muxNewMsgPart: //New msg from remote connection
					err = s.newMsg(connection, pack)
					if err != nil {
						log.Println("mux: read session connection New msg err", err)
						_ = connection.Close()
					}
					continue
				case muxNewConnOk: //connection ok
					connection.connStatusOkCh <- struct{}{}
					continue
				case muxNewConnFail:
					connection.connStatusFailCh <- struct{}{}
					continue
				case muxMsgSendOk:
					if atomic.LoadInt32(&connection.isClose) != 0 {
						continue
					}
					connection.sendWindow.SetSize(pack.window)
					continue
				case muxConnClose: //close the connection
					atomic.StoreInt32(&connection.closingFlag, 1)
					connection.receiveWindow.Stop() // close signal to receive window
					continue
				}
			} else if pack.flag == muxConnClose {
				continue
			}
			muxPack.Put(pack)
		}
	}()
}

func (s *Mux) newMsg(connection *conn, pack *muxPackager) (err error) {
	defer PanicHandler()
	if atomic.LoadInt32(&connection.isClose) != 0 {
		err = io.ErrClosedPipe
		return
	}
	//insert into queue
	if pack.flag == muxNewMsgPart {
		err = connection.receiveWindow.Write(pack.content, pack.length, true, pack.id)
	}
	if pack.flag == muxNewMsg {
		err = connection.receiveWindow.Write(pack.content, pack.length, false, pack.id)
	}
	return
}

func (s *Mux) Close() (err error) {
	defer PanicHandler()
	if atomic.LoadInt32(&s.IsClose) != 0 {
		return errors.New("the mux has closed")
	}
	atomic.StoreInt32(&s.IsClose, 1)
	log.Println("close mux")
	s.connMap.Close()
	//s.connMap = nil
	s.closeChan <- struct{}{}
	close(s.newConnCh)
	// while target host close socket without finish steps, conn.Close method maybe blocked
	// and tcp status change to CLOSE WAIT or TIME WAIT, so we close it in other goroutine
	_ = s.conn.SetDeadline(time.Now().Add(time.Second * 5))
	go func() {
		defer PanicHandler()
		s.conn.Close()
		s.bw.Close()
	}()
	s.release()
	return
}

func (s *Mux) release() {
	defer PanicHandler()
	for {
		pack := s.writeQueue.TryPop()
		if pack == nil {
			break
		}
		if pack.basePackager.content != nil {
			windowBuff.Put(pack.basePackager.content)
		}
		muxPack.Put(pack)
	}
	for {
		connection := s.newConnQueue.TryPop()
		if connection == nil {
			break
		}
		connection = nil
	}
	s.writeQueue.Stop()
	s.newConnQueue.Stop()
}

// Get New connId as unique flag
func (s *Mux) getId() (id int32) {
	defer PanicHandler()
	//Avoid going beyond the scope
	if (math.MaxInt32 - s.id) < 10000 {
		atomic.StoreInt32(&s.id, 0)
	}
	id = atomic.AddInt32(&s.id, 1)
	if _, ok := s.connMap.Get(id); ok {
		return s.getId()
	}
	return
}

type bandwidth struct {
	readBandwidth uint64 // store in bits, but it's float64
	readStart     time.Time
	lastReadStart time.Time
	bufLength     uint32
	fd            *os.File
	calcThreshold uint32
}

func NewBandwidth(fd *os.File) *bandwidth {
	defer PanicHandler()
	return &bandwidth{fd: fd}
}

func (Self *bandwidth) StartRead() {
	defer PanicHandler()
	if Self.readStart.IsZero() {
		Self.readStart = time.Now()
	}
	if Self.bufLength >= Self.calcThreshold {
		Self.lastReadStart, Self.readStart = Self.readStart, time.Now()
		Self.calcBandWidth()
	}
}

func (Self *bandwidth) SetCopySize(n uint16) {
	defer PanicHandler()
	Self.bufLength += uint32(n)
}

func (Self *bandwidth) calcBandWidth() {
	defer PanicHandler()
	t := Self.readStart.Sub(Self.lastReadStart)
	bufferSize, err := sysGetSock(Self.fd)
	if err != nil {
		log.Println(err)
		Self.bufLength = 0
		return
	}
	if Self.bufLength >= uint32(bufferSize) {
		atomic.StoreUint64(&Self.readBandwidth, math.Float64bits(float64(Self.bufLength)/t.Seconds()))
		// calculate the whole socket buffer, the time meaning to fill the buffer
	} else {
		Self.calcThreshold = uint32(bufferSize)
	}
	// socket buffer size is bigger than bufLength, so we don't calculate it
	Self.bufLength = 0
}

func (Self *bandwidth) Get() (bw float64) {
	defer PanicHandler()
	// The zero value, 0 for numeric types
	bw = math.Float64frombits(atomic.LoadUint64(&Self.readBandwidth))
	if bw <= 0 {
		bw = 0
	}
	return
}

func (Self *bandwidth) Close() error {
	defer PanicHandler()
	return Self.fd.Close()
}

const counterBits = 4
const counterMask = 1<<counterBits - 1

func newLatencyCounter() *latencyCounter {
	return &latencyCounter{
		buf:     make([]float64, 1<<counterBits, 1<<counterBits),
		headMin: 0,
	}
}

type latencyCounter struct {
	buf []float64 //buf is a fixed length ring buffer,
	// if buffer is full, New value will replace the oldest one.
	headMin uint8 //head indicate the head in ring buffer,
	// in meaning, slot in list will be replaced;
	// min indicate this slot value is minimal in list.

	// we delineate the effective range with three times the minimum latency
	// average of effective latency for all current data as a mux latency
}

func (Self *latencyCounter) unpack(idxs uint8) (head, min uint8) {
	head = (idxs >> counterBits) & counterMask
	// we Set head is 4 bits
	min = idxs & counterMask
	return
}

func (Self *latencyCounter) pack(head, min uint8) uint8 {
	return head<<counterBits |
		min&counterMask
}

func (Self *latencyCounter) add(value float64) {
	head, min := Self.unpack(Self.headMin)
	Self.buf[head] = value
	if head == min {
		min = Self.minimal()
		//if head equals min, means the min slot already be replaced,
		// so we need to find another minimal value in the list,
		// and change the min indicator
	}
	if Self.buf[min] > value {
		min = head
	}
	head++
	Self.headMin = Self.pack(head, min)
}

func (Self *latencyCounter) minimal() (min uint8) {
	var val float64
	var i uint8
	for i = 0; i < counterMask; i++ {
		if Self.buf[i] > 0 {
			if val > Self.buf[i] {
				val = Self.buf[i]
				min = i
			}
		}
	}
	return
}

func (Self *latencyCounter) Latency(value float64) (latency float64) {
	Self.add(value)
	latency = Self.countSuccess()
	return
}

const lossRatio = 3

func (Self *latencyCounter) countSuccess() (successRate float64) {
	var i, success uint8
	_, min := Self.unpack(Self.headMin)
	for i = 0; i < counterMask; i++ {
		if Self.buf[i] <= lossRatio*Self.buf[min] && Self.buf[i] > 0 {
			success++
			successRate += Self.buf[i]
		}
	}
	// counting all the data in the ring buf, except zero
	successRate = successRate / float64(success)
	return
}
