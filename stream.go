package smux

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

// Stream implements net.Conn
type Stream struct {
	id   uint32
	sess *Session

	buffers    [][]byte
	bufferSize uint32   // to track total num of bytes in buffers
	heads      [][]byte // slice heads kept for recycle

	bufferLock sync.Mutex
	frameSize  int

	// notify a read event
	chReadEvent chan struct{}

	// flag the stream has closed
	die     chan struct{}
	dieOnce sync.Once

	// FIN
	chFinEvent   chan struct{}
	finEventOnce sync.Once

	// deadlines
	readDeadline  atomic.Value
	writeDeadline atomic.Value

	// count writes, for tx queue shaper
	numWrite uint64

	// remote window, remote will update this value perodically
	// for setting per-stream max socket buffer, default to 64KB
	numRead        uint32        // temporary recording of bytes read
	sendWindow     uint32        // remote window size
	chWindowChange chan struct{} // notify of remote window change
}

// newStream initiates a Stream struct
func newStream(id uint32, frameSize int, sess *Session) *Stream {
	s := new(Stream)
	s.id = id
	s.chReadEvent = make(chan struct{}, 1)
	s.chWindowChange = make(chan struct{}, 1)
	s.frameSize = frameSize
	s.sess = sess
	s.die = make(chan struct{})
	s.chFinEvent = make(chan struct{})
	s.sendWindow = uint32(sess.config.MaxStreamBuffer)
	return s
}

// ID returns the unique stream ID.
func (s *Stream) ID() uint32 {
	return s.id
}

// Read implements net.Conn
func (s *Stream) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	var sendWindowUpdate bool
	var recvWindow uint32
	for {
		s.bufferLock.Lock()
		if len(s.buffers) > 0 {
			n = copy(b, s.buffers[0])
			s.buffers[0] = s.buffers[0][n:]
			if len(s.buffers[0]) == 0 {
				s.buffers[0] = nil
				s.buffers = s.buffers[1:]
				// full recycle
				defaultAllocator.Put(s.heads[0])
				s.heads = s.heads[1:]
			}
		}
		s.bufferSize -= uint32(n)
		s.numRead += uint32(n)
		if s.numRead >= uint32(s.sess.config.MaxStreamBuffer/2) && s.bufferSize < uint32(s.sess.config.MaxStreamBuffer) {
			sendWindowUpdate = true
			recvWindow = uint32(s.sess.config.MaxStreamBuffer) - s.bufferSize
			s.numRead = 0 // reset
		}
		//log.Println("numread:", s.numRead, "s.buffersize:", s.bufferSize)
		s.bufferLock.Unlock()

		// create timer
		var timer *time.Timer
		var deadline <-chan time.Time
		if d, ok := s.readDeadline.Load().(time.Time); ok && !d.IsZero() {
			timer = time.NewTimer(time.Until(d))
			defer timer.Stop()
			deadline = timer.C
		}

		if n > 0 {
			s.sess.returnTokens(n)
			if sendWindowUpdate {
				frame := newFrame(cmdWND, s.id)
				frame.data = make([]byte, szWindowUpdate)
				binary.LittleEndian.PutUint32(frame.data, recvWindow)
				s.sess.writeFrameInternal(frame, deadline, 0)
			}
			return n, nil
		}

		select {
		case <-s.chReadEvent:
			continue
		case <-s.chFinEvent:
			return 0, errors.WithStack(io.EOF)
		case <-s.sess.chSocketReadError:
			return 0, s.sess.socketReadError.Load().(error)
		case <-s.sess.chProtoError:
			return 0, s.sess.protoError.Load().(error)
		case <-deadline:
			return 0, errors.WithStack(errTimeout)
		case <-s.die:
			return 0, errors.WithStack(io.ErrClosedPipe)
		}
	}
}

// Write implements net.Conn
func (s *Stream) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	var deadline <-chan time.Time
	if d, ok := s.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	// check if stream has closed
	select {
	case <-s.die:
		return 0, errors.WithStack(io.ErrClosedPipe)
	default:
	}

	// frame split and transmit process
	var bts []byte
	sent := 0
	frame := newFrame(cmdPSH, s.id)

	for {
		// window control
		win := atomic.LoadUint32(&s.sendWindow)
		if win > uint32(len(b)) {
			bts = b
			b = nil
		} else {
			bts = b[:win] // win=0 still works
			b = b[win:]
		}

		if win > 0 { // shrink window
			atomic.AddUint32(&s.sendWindow, ^uint32(len(bts)-1))
		}

		for len(bts) > 0 {
			sz := len(bts)
			if sz > s.frameSize {
				sz = s.frameSize
			}
			frame.data = bts[:sz]
			bts = bts[sz:]
			n, err := s.sess.writeFrameInternal(frame, deadline, s.numWrite)
			s.numWrite++
			sent += n
			if err != nil {
				return sent, errors.WithStack(err)
			}
		}

		// if there is data remaining to be sent
		// wait for stream close or window change or deadline
		if len(b) > 0 {
			select {
			case <-s.die:
				return sent, errors.WithStack(io.ErrClosedPipe)
			case <-deadline:
				return sent, errors.WithStack(errTimeout)
			case <-s.chWindowChange:
				continue
			}
		} else {
			return sent, nil
		}
	}
}

// Close implements net.Conn
func (s *Stream) Close() error {
	var once bool
	var err error
	s.dieOnce.Do(func() {
		close(s.die)
		once = true
	})

	if once {
		_, err = s.sess.writeFrame(newFrame(cmdFIN, s.id))
		s.sess.streamClosed(s.id)
		return err
	} else {
		return errors.WithStack(io.ErrClosedPipe)
	}
}

// GetDieCh returns a readonly chan which can be readable
// when the stream is to be closed.
func (s *Stream) GetDieCh() <-chan struct{} {
	return s.die
}

// SetReadDeadline sets the read deadline as defined by
// net.Conn.SetReadDeadline.
// A zero time value disables the deadline.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	return nil
}

// SetWriteDeadline sets the write deadline as defined by
// net.Conn.SetWriteDeadline.
// A zero time value disables the deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	return nil
}

// SetDeadline sets both read and write deadlines as defined by
// net.Conn.SetDeadline.
// A zero time value disables the deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return errors.WithStack(err)
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// session closes
func (s *Stream) sessionClose() { s.dieOnce.Do(func() { close(s.die) }) }

// LocalAddr satisfies net.Conn interface
func (s *Stream) LocalAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		LocalAddr() net.Addr
	}); ok {
		return ts.LocalAddr()
	}
	return nil
}

// RemoteAddr satisfies net.Conn interface
func (s *Stream) RemoteAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		RemoteAddr() net.Addr
	}); ok {
		return ts.RemoteAddr()
	}
	return nil
}

// pushBytes append buf to buffers
func (s *Stream) pushBytes(buf []byte) (written int, err error) {
	s.bufferLock.Lock()
	s.buffers = append(s.buffers, buf)
	s.heads = append(s.heads, buf)
	s.bufferSize += uint32(len(buf))
	s.bufferLock.Unlock()
	return
}

// recycleTokens transform remaining bytes to tokens(will truncate buffer)
func (s *Stream) recycleTokens() (n int) {
	s.bufferLock.Lock()
	for k := range s.buffers {
		n += len(s.buffers[k])
		defaultAllocator.Put(s.heads[k])
	}
	s.buffers = nil
	s.heads = nil
	s.bufferSize = 0
	s.bufferLock.Unlock()
	return
}

// notify read event
func (s *Stream) notifyReadEvent() {
	select {
	case s.chReadEvent <- struct{}{}:
	default:
	}
}

// update window value and notify window change
func (s *Stream) notifyWindowChangeEvent(window uint32) {
	atomic.StoreUint32(&s.sendWindow, window)
	select {
	case s.chWindowChange <- struct{}{}:
	default:
	}
}

// mark this stream has been closed in protocol
func (s *Stream) fin() {
	s.finEventOnce.Do(func() {
		close(s.chFinEvent)
	})
}
