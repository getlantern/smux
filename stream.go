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

	buffers [][]byte
	heads   [][]byte // slice heads kept for recycle

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

	// per stream sliding window control
	numRead    uint32        // temporary tracking of bytes read, capped to half window
	numWritten uint32        // count num of bytes written
	numSink    uint32        // peer data consumed, starting from 0
	chSink     chan struct{} // notify of remote data consuming
}

// newStream initiates a Stream struct
func newStream(id uint32, frameSize int, sess *Session) *Stream {
	s := new(Stream)
	s.id = id
	s.chReadEvent = make(chan struct{}, 1)
	s.chSink = make(chan struct{}, 1)
	s.frameSize = frameSize
	s.sess = sess
	s.die = make(chan struct{})
	s.chFinEvent = make(chan struct{})
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

	for {
		var sendSink bool
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
		s.numRead += uint32(n)
		// if more than half of buffer has consumed, send read ack to peer
		// based on round-trip time of ACK, continous flowing data
		// won't slow down because of waiting for ACK, as long as the
		// consumer keeps on reading data
		if s.numRead >= uint32(s.sess.config.MaxStreamBuffer/2) {
			sendSink = true
			s.numRead %= uint32(s.sess.config.MaxStreamBuffer / 2)
		}
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
			if sendSink {
				frame := newFrame(byte(s.sess.config.Version), cmdSINK, s.id)
				frame.data = make([]byte, szCmdSINK)
				binary.LittleEndian.PutUint32(frame.data, uint32(s.sess.config.MaxStreamBuffer/2))
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
	// check empty input
	if len(b) == 0 {
		return 0, nil
	}

	// check if stream has closed
	select {
	case <-s.die:
		return 0, errors.WithStack(io.ErrClosedPipe)
	default:
	}

	// create write deadline timer
	var deadline <-chan time.Time
	if d, ok := s.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	// frame split and transmit process
	sent := 0
	frame := newFrame(byte(s.sess.config.Version), cmdPSH, s.id)

	for {
		// per stream sliding window control
		// [.... [sink... numWrite] ... win... ]
		// [.... [sink.........MaxStreamBuffer]]
		var bts []byte
		win := s.sess.config.MaxStreamBuffer - int(s.numWritten-atomic.LoadUint32(&s.numSink))
		if win > 0 {
			if win > len(b) {
				bts = b
				b = nil
			} else {
				bts = b[:win]
				b = b[win:]
			}

			for len(bts) > 0 {
				sz := len(bts)
				if sz > s.frameSize {
					sz = s.frameSize
				}
				frame.data = bts[:sz]
				bts = bts[sz:]
				n, err := s.sess.writeFrameInternal(frame, deadline, uint64(s.numWritten))
				s.numWritten += uint32(sz)
				sent += n
				if err != nil {
					return sent, errors.WithStack(err)
				}
			}
		}

		// if there is any data remaining to be sent
		// wait until stream closes, window changes or deadline reached
		// this blocking behavior will inform upper layer to do flow control
		if len(b) > 0 {
			select {
			case <-s.die:
				return sent, errors.WithStack(io.ErrClosedPipe)
			case <-deadline:
				return sent, errors.WithStack(errTimeout)
			case <-s.chSink:
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
		_, err = s.sess.writeFrame(newFrame(byte(s.sess.config.Version), cmdFIN, s.id))
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

// notify n bytes has consumed by peer
func (s *Stream) notifyBytesSink(n uint32) {
	atomic.AddUint32(&s.numSink, n)
	select {
	case s.chSink <- struct{}{}:
	default:
	}
}

// mark this stream has been closed in protocol
func (s *Stream) fin() {
	s.finEventOnce.Do(func() {
		close(s.chFinEvent)
	})
}
