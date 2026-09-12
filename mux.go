package nps_mux

import (
	"errors"
	"io"
	"log"
	"math"
	"net"
	"os"
	"sync/atomic"
	"syscall"
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

type Mux struct {
	latency uint64 // we store latency in bits, but it's float64
	net.Listener
	conn               net.Conn
	connMap            *connMap
	newConnCh          chan *conn
	id                 int32
	closeChan          chan struct{}
	IsClose            int32 // 改为原子类型
	counter            *latencyCounter
	bw                 *bandwidth
	pingCh             chan []byte
	pingCheckTime      uint32 // we check the ping per 5s
	pingCheckThreshold uint32
	connType           string
	writeQueue         priorityQueue
	newConnQueue       connQueue
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
		return nil, errors.New("accept error,the mux has closed")
	}
	conn := <-s.newConnCh
	if conn == nil {
		return nil, errors.New("accept error,the conn is nil")
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
		_ = s.closeWithReason("encode_error", err)
		return
	}
	s.writeQueue.Push(pack)
}

func (s *Mux) writeSession() {
	defer PanicHandler()
	go func() {
		defer PanicHandler()
		// 具备一直执行的条件，会死循环导致CPU暴增
		for {
			select {
			case <-s.closeChan:
				return
			default:
				if atomic.LoadInt32(&s.IsClose) != 0 {
					return
				}
				pack := s.writeQueue.Pop()
				if pack == nil {
					continue
				}
				err := pack.Pack(s.conn)
				muxPack.Put(pack)
				if err != nil {
					_ = s.closeWithReason("write_error", err)
					return
				}
			}
		}
	}()
}

func (s *Mux) ping() {
	defer PanicHandler()
	// 发送 ping包，和检查超时机制
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
				_ = s.closeWithReason("ping_timeout", nil)
				// more than limit times not receive the ping return package,
				// mux conn is damaged, maybe a packet drop, close it
				break
			}
			now, _ = time.Now().UTC().MarshalText()
			s.sendInfo(muxPingFlag, muxPing, now)
			atomic.AddUint32(&s.pingCheckTime, 1) // 次数加1,连续超过阈值就close了，说明长时间未响应
		}
	}()

	// 接收ping响应
	go func() {
		defer PanicHandler()
		var now time.Time
		var data []byte
	pingLoop:
		for {
			if atomic.LoadInt32(&s.IsClose) != 0 {
				break
			}
			select {
			case data = <-s.pingCh: // channel处理连接返回的ping包响应
				atomic.StoreUint32(&s.pingCheckTime, 0) // 响应成功就重置这个计数器
				err := now.UnmarshalText(data)
				if err != nil {
					log.Println("mux: ping response Unmarshal err", err)
				}
				latency := time.Now().UTC().Sub(now).Seconds()
				if latency > 0 {
					atomic.StoreUint64(&s.latency, math.Float64bits(s.counter.Latency(latency)))
					// convert float64 to bits, store it atomic
					//log.Println("ping", math.Float64frombits(atomic.LoadUint64(&s.latency)))
				}
				//if cap(data) > 0 && atomic.LoadInt32(&s.IsClose) == 0 {
				//	windowBuff.Put(data)
				//}
			case <-s.closeChan:
				break pingLoop
			}
		}
	}()
}

func (s *Mux) readSession() {
	defer PanicHandler()
	go func() {
		defer PanicHandler()
		for {
			select {
			case <-s.closeChan:
				return
			default:
				if atomic.LoadInt32(&s.IsClose) != 0 {
					return
				}
				connection := s.newConnQueue.Pop()
				if connection == nil {
					continue // 避免死循环
				}
				s.connMap.Set(connection.connId, connection)
				// safe send to avoid panic when channel already closed
				if !s.trySendNewConn(connection) { //it has been Set before send ok
					break
				}
				s.sendInfo(muxNewConnOk, connection.connId, nil)
			}
		}
	}()
	go func() {
		defer PanicHandler()
		var pack *muxPackager
		var l uint16
		var err error
		for {
			select {
			case <-s.closeChan:
				return
			default:
				if atomic.LoadInt32(&s.IsClose) != 0 {
					return
				}
				pack = muxPack.Get()
				s.bw.StartRead()
				l, err = pack.UnPack(s.conn)
				if err != nil {
					muxPack.Put(pack)
					_ = s.closeWithReason("read_error", err)
					return
				}
				s.bw.SetCopySize(l)

				switch pack.flag {
				case muxNewConn:
					connection := NewConn(pack.id, s)
					s.newConnQueue.Push(connection)
					continue
				case muxPingFlag:
					// 复制 content 以避免重用
					contentCopy := make([]byte, len(pack.content))
					copy(contentCopy, pack.content)
					s.sendInfo(muxPingReturn, muxPing, contentCopy)
					windowBuff.Put(pack.content)
					muxPack.Put(pack)
					continue
				case muxPingReturn:
					// 复制 content 以避免重用
					contentCopy := make([]byte, len(pack.content))
					copy(contentCopy, pack.content)
					s.pingCh <- contentCopy
					windowBuff.Put(pack.content)
					muxPack.Put(pack)
					continue
				}

				if connection, ok := s.connMap.Get(pack.id); ok && atomic.LoadInt32(&connection.isClose) == 0 {
					switch pack.flag {
					case muxNewMsg, muxNewMsgPart:
						err = s.newMsg(connection, pack)
						if err != nil {
							log.Println("mux: read session connection new msg err", err)
							_ = connection.Close()
						}
						continue
					case muxNewConnOk:
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
					case muxConnClose:
						atomic.StoreInt32(&connection.closingFlag, 1)
						connection.receiveWindow.Stop()
						continue
					}
				} else if pack.flag == muxConnClose {
					continue
				}
				muxPack.Put(pack)
			}
		}
	}()
}

// 尝试向 newConnCh 发送，若通道已关闭则返回 false，避免 panic
func (s *Mux) trySendNewConn(c *conn) bool {
	if atomic.LoadInt32(&s.IsClose) != 0 {
		return false
	}
	select {
	case s.newConnCh <- c:
		return true
	default:
		return false // 通道满或已关闭（实际无法区分，但不会 panic）
	}
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

func (s *Mux) Close() error {
	return s.closeWithReason("local_close", nil)
}

func closeErrorKind(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "tcp_reset"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "other"
}

// Record only the first close trigger; subsequent socket errors are consequences.
func (s *Mux) closeWithReason(reason string, cause error) (err error) {
	defer PanicHandler()
	if !atomic.CompareAndSwapInt32(&s.IsClose, 0, 1) {
		return errors.New("the mux has closed")
	}
	log.Printf("mux_closed local=%s remote=%s transport=%s reason=%s error_kind=%s missed_pings=%d threshold=%d err=%v", s.conn.LocalAddr(), s.conn.RemoteAddr(), s.connType, reason, closeErrorKind(cause), atomic.LoadUint32(&s.pingCheckTime), s.pingCheckThreshold, cause)
	s.release() // 先释放队列
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
	// 原子读取当前值
	current := atomic.LoadInt32(&s.id)
	// 原子化检查并重置
	if (math.MaxInt32 - current) < 10000 {
		if atomic.CompareAndSwapInt32(&s.id, current, 0) {
			current = 0
		} else {
			// 如果CAS失败，说明其他goroutine已经修改了值，重试
			return s.getId()
		}
	}

	// 原子递增
	id = atomic.AddInt32(&s.id, 1)

	// 检查是否已存在
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
