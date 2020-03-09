package zookeeper

import (
	"bufio"
	"container/list"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	// req header is length(4) xid(4) op(4)
	reqHeaderSize = 12

	pingXid = -2
)

func makeRand() *rand.Rand {
	p := make([]byte, 8)
	if _, err := crand.Read(p); err != nil {
		panic(err)
	}

	return rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(p))))
}

type Dialer interface {
	Dial(net, addr string) (net.Conn, error)
}

type options struct {
	dialer         Dialer
	sessionTimeout time.Duration
}

type ConnectOption func(o *options)

// session maintains the state for the current zookeeper
// session, it is carefully used by seperate goroutines
// to ensure that races do not happen. Authenticate is only
// ran when recv/send/ping loops are not running, thus allowing
// it to be used without locks or atomics.
type session struct {
	// read and written by authenticate
	password [16]byte
	// read and written by authenticate, read by recv, send and ping
	timeout time.Duration
	// written by recv, read from authenticate
	zxid int64
	// read and written by authenticate
	sessionID int64
}

type response struct {
	buf []byte
	err error
}

type responsePacket interface {
	deocde(p []byte) error
}

type requestPacket interface {
	encode(c *coder)
}

type request struct {
	xid  int32 // set after it has been written
	op   opCode
	body []byte
	resp chan *response
}

type requestQueue struct {
	mu    sync.Mutex
	items list.List
}

func (r *requestQueue) push(req *request) {
	r.mu.Lock()
	r.items.PushBack(req)
	r.mu.Unlock()
}

func (r *requestQueue) pop() *request {
	r.mu.Lock()
	e := r.items.Front()
	r.items.Remove(e)
	r.mu.Unlock()
	return e.Value.(*request)
}

type Client struct {
	hosts          []string
	dialer         Dialer
	rand           *rand.Rand
	defaultTimeout time.Duration

	session session
	// current xid which is only read + written by sendPacket
	xid int32

	// zookeeper responds to requests in order they are sent,
	// if we receive an out of order request then we are out of
	// sync or lost a request so we will re authenticate the session.
	requests requestQueue

	writes chan *request
}

func Connect(ctx context.Context, addrs []string, opts ...ConnectOption) *Client {
	hosts := make([]string, len(addrs))
	copy(hosts, addrs)

	rand := makeRand()
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	o := options{
		dialer:         &net.Dialer{},
		sessionTimeout: 15 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}

	c := &Client{
		hosts:  hosts,
		rand:   rand,
		dialer: o.dialer,
		writes: make(chan *request),
		session: session{
			timeout: o.sessionTimeout,
		},
		defaultTimeout: o.sessionTimeout,
	}
	go c.loop(ctx, hosts)
	return c
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

var (
	ErrLostSession = errors.New("zookeeper: lost session to zookeeper")
)

func (c *Client) resetSession() {
	c.session = session{
		timeout: c.defaultTimeout,
	}
	// TODO: can we reset xid on session reset?
	c.xid = 0

	for e := c.requests.items.Back(); e != nil; e = e.Next() {
		req := e.Value.(*request)
		req.resp <- &response{err: ErrLostSession}
		c.requests.items.Remove(e)
	}
}

func (c *Client) run(ctx context.Context, addr string) error {
	conn, br, err := c.authenticate(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// TODO: send a ping before starting the loops to ensure
	// that we have a valid connection to zookeeper.

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return c.ping(ctx, conn)
	})

	eg.Go(func() error {
		return c.recv(ctx, conn, br)
	})

	eg.Go(func() error {
		return c.send(ctx, conn)
	})

	return eg.Wait()
}

func (c *Client) authenticate(ctx context.Context, addr string) (conn net.Conn, _ *bufio.Reader, err error) {
	conn, err = c.dialer.Dial("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("authenticate: unable to dial connection: %w", err)
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	co := &coder{buf: make([]byte, 4, 44)}
	req := &connectRequest{
		lastZxid:  c.session.zxid,
		sessionID: c.session.sessionID,
		timeout:   int32(c.session.timeout.Milliseconds()),
		password:  c.session.password[:],
	}
	req.encode(co)

	be.PutUint32(co.buf[:4], uint32(co.len()-4))

	// TODO: deadlines
	if _, err := conn.Write(co.buf); err != nil {
		return nil, nil, fmt.Errorf("authenticate: write connect request: %w", err)
	}

	br := bufio.NewReader(conn)

	buf := co.buf
	if _, err := io.ReadFull(br, buf[:4]); err != nil {
		return nil, nil, fmt.Errorf("authenticate: unable to read response length: %w", err)
	}

	n := int(be.Uint32(buf[:4]))
	if cap(buf) >= n {
		buf = buf[:n]
	} else {
		buf = make([]byte, n)
	}

	if _, err := io.ReadFull(br, buf); err != nil {
		return nil, nil, fmt.Errorf("authenticate: unable to read response: %w", err)
	}

	var resp connectResponse
	if err := resp.decode(buf); err != nil {
		return nil, nil, fmt.Errorf("authenticate: unable to decode response: %w", err)
	}

	c.session.sessionID = resp.sessionID
	c.session.timeout = time.Duration(resp.timeout) * time.Millisecond
	copy(c.session.password[:], resp.password)
	log.Printf("authenticated sessionID=%d timeout=%v", resp.sessionID, c.session.timeout)

	return conn, br, nil
}

func (c *Client) loop(ctx context.Context, addrs []string) {
	var i int
	for {
		if err := c.run(ctx, addrs[i%len(addrs)]); err != nil {
			if isContextErr(err) {
				return
			} else if errors.Is(err, io.ErrUnexpectedEOF) {
				// we lost our session, reset and flush requests
				c.resetSession()
			}

			// TODO: remove logs
			log.Printf("zookeeper: %v", err)
			// TODO: backoff
		}
		i++
	}
}

type deadlineReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type deadlineWriter interface {
	io.Writer
	SetWriteDeadline(time.Time) error
}

func (c *Client) readResponse(ctx context.Context, r io.Reader, headerBuf []byte) ([]byte, error) {
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, fmt.Errorf("recv: unable to read packet header: %w", err)
	}

	// TODO: check this value is sensible
	n := int32(be.Uint32(headerBuf[:4]))
	if n < 0 {
		return nil, fmt.Errorf("recv: invalid packet length from zookeeper: %d", n)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("recv: unable to read packet body: %w", err)
	}

	return buf, nil
}

func waitUntilPacket(br *bufio.Reader) error {
	_, err := br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return br.UnreadByte()
}

func (c *Client) recv(ctx context.Context, r deadlineReader, br *bufio.Reader) error {
	recvTimeout := time.Duration(c.session.timeout * 2 / 3)
	headerBuf := make([]byte, 4)

	for {
		if err := r.SetReadDeadline(time.Time{}); err != nil {
			return fmt.Errorf("recv: unable to set read deadline: %w", err)
		}
		// we cant use a channel to signal to do a read because zookeeper will send us
		// notifications about watches and things. Instead we just wait for a byte to be
		// available, then start the timer to ensure we read a full packet in the recvTimeout.
		if err := waitUntilPacket(br); err != nil {
			return fmt.Errorf("recv: erorr whilst waiting to read packet from zookeeper: %w", err)
		}

		if err := r.SetReadDeadline(time.Now().Add(recvTimeout)); err != nil {
			return fmt.Errorf("recv: unable to set read deadline: %w", err)
		}

		buf, err := c.readResponse(ctx, br, headerBuf)
		if err != nil {
			return err
		}

		var head respHeader
		if err := head.decode(buf); err != nil {
			return fmt.Errorf("recv: unable to decode packet header: %w", err)
		}

		log.Printf("xid=%d zxid=%d err=%d", head.xid, head.zxid, head.errCode)

		req := c.requests.pop()
		if req.xid != head.xid {
			return fmt.Errorf("recv: recieved out of order packet: expected xid %d got %d", req.xid, head.xid)
		}

		c.session.zxid = head.zxid

		resp := &response{
			// response header is xid(4) zxid(8) errCode(4) body follows
			buf: buf[16:],
		}

		select {
		case req.resp <- resp:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// sendPacket encodes and writes a request packet to zookeeper, including the framing
// format. It sets req.xid and stores the request in the queue.
func (c *Client) sendPacket(ctx context.Context, w io.Writer, req *request) error {
	if req.xid == 0 {
		// we must send out requests with monotonically incramenting xids and ensure
		// that we maintain the order in the request queue
		req.xid = c.xid
		c.xid++
	}

	buf := make([]byte, reqHeaderSize)
	be.PutUint32(buf[4:], uint32(req.xid))
	be.PutUint32(buf[8:], uint32(req.op))

	wv := net.Buffers{buf}

	n := 8
	if len(req.body) > 0 {
		n += len(req.body)
		wv = append(wv, req.body)
	}

	be.PutUint32(buf, uint32(n))

	if _, err := wv.WriteTo(w); err != nil {
		return err
	}
	c.requests.push(req)

	log.Printf("wrote xid=%d", req.xid)

	return nil
}

func (c *Client) send(ctx context.Context, w io.Writer) error {
	for {
		select {
		case req := <-c.writes:
			if err := c.sendPacket(ctx, w, req); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) ping(ctx context.Context, w io.Writer) error {
	interval := c.session.timeout / 3
	recvTimeout := time.Duration(c.session.timeout * 2 / 3)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}

		log.Println("sending ping")
		req := &request{
			resp: make(chan *response, 1),
			xid:  pingXid,
			op:   opPing,
		}

		select {
		case c.writes <- req:
		case <-ctx.Done():
			return ctx.Err()
		}

		timer.Reset(recvTimeout)

		select {
		case <-req.resp:
			log.Println("PING!")
		case <-ctx.Done():
			return ctx.Err()
		}

		timer.Reset(interval)
	}

	return nil
}

func encodePacket(p requestPacket) []byte {
	if p == nil {
		return nil
	}

	var c coder
	p.encode(&c)
	return c.buf
}

func (c *Client) doRequest(ctx context.Context, op opCode, in requestPacket, out responsePacket) error {
	req := &request{
		resp: make(chan *response, 1),
		body: encodePacket(in),
		op:   op,
	}
	select {
	case c.writes <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-req.resp:
		if resp.err != nil {
			return fmt.Errorf("zookeeper: unable to execute %s request: %w", op, resp.err)
		}

		if out != nil {
			if err := out.deocde(resp.buf); err != nil {
				return fmt.Errorf("zookeeper: unable to decode %s response: %w", op, resp.err)
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
