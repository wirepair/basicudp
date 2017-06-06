// An attempt at an optimized udp client/server implementation that has clients sending 10pps.
// run the server: go build && main -server -num 5000
// run the client: go build && main -num 5000
// i was only able to get 9000 clients sending for 30 seconds with 0 packet loss in windows
// after that i started get drops
//
// author: isaac dawson @ https://twitter.com/_wirepair

package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"github.com/pkg/profile"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_PACKET_BYTES = 1220
	SOCKET_BUF_SIZE  = 1024 * 1024
)

var serverMode bool
var maxClients int
var runTime float64
var runProfile bool
var randomizeStart bool

var clientTotalSend uint64
var clientTotalRecv uint64

var addr = &net.UDPAddr{IP: net.ParseIP("::1"), Port: 40000}

func init() {
	flag.BoolVar(&serverMode, "server", false, "pass this flag to run the server")
	flag.BoolVar(&runProfile, "prof", false, "pass this flag to enable profiling")
	flag.BoolVar(&randomizeStart, "rand", false, "pass this flag to randomize client startups")
	flag.IntVar(&maxClients, "num", 64, "number of clients to serve or to create")
	flag.Float64Var(&runTime, "runtime", 5.0, "how long to run clients for/clear client buffer in seconds")
}

// our struct for passing data and client addresses around
type netcodeData struct {
	data []byte
	from *net.UDPAddr
}

// allows for supporting custom handlers
type NetcodeRecvHandler func(data *netcodeData)

type NetcodeConn struct {
	conn     *net.UDPConn  // the underlying connection
	closeCh  chan struct{} // used for closing the connection/signaling
	isClosed bool          // is this connection open/closed?

	maxBytes int       // maximum allowed bytes for read/write
	xmitBuf  sync.Pool // re-use recv buf to reduce allocs

	recvSize      int
	sendSize      int
	recvHandlerFn NetcodeRecvHandler // allow custom recv handlers
}

// Creates a new netcode connection
func NewNetcodeConn() *NetcodeConn {
	c := &NetcodeConn{}
	c.closeCh = make(chan struct{})
	c.isClosed = true
	c.maxBytes = MAX_PACKET_BYTES
	return c
}

// set a custom recv handler (must be called before Dial or Listen)
func (c *NetcodeConn) SetRecvHandler(recvHandlerFn NetcodeRecvHandler) {
	c.recvHandlerFn = recvHandlerFn
}

// Write to the connection
func (c *NetcodeConn) Write(b []byte) (int, error) {
	if c.isClosed {
		return -1, errors.New("unable to write, socket has been closed")
	}
	return c.conn.Write(b)
}

// Write to an address (only usable via Listen)
func (c *NetcodeConn) WriteTo(b []byte, to *net.UDPAddr) (int, error) {
	if c.isClosed {
		return -1, errors.New("unable to write, socket has been closed")
	}
	return c.conn.WriteToUDP(b, to)
}

// Shutdown time.
func (c *NetcodeConn) Close() error {
	if !c.isClosed {
		close(c.closeCh)
	}
	c.isClosed = true
	return c.conn.Close()
}

// Dial the server
func (c *NetcodeConn) Dial(address *net.UDPAddr) error {
	var err error

	if c.recvHandlerFn == nil {
		return errors.New("packet handler must be set before calling listen")
	}

	c.closeCh = make(chan struct{})
	c.conn, err = net.DialUDP(address.Network(), nil, address)
	if err != nil {
		return err
	}
	c.sendSize = SOCKET_BUF_SIZE
	c.recvSize = SOCKET_BUF_SIZE
	c.create()
	return nil
}

// Listen for connections on address
func (c *NetcodeConn) Listen(address *net.UDPAddr) error {
	var err error

	if c.recvHandlerFn == nil {
		return errors.New("packet handler must be set before calling listen")
	}

	c.conn, err = net.ListenUDP(address.Network(), address)
	if err != nil {
		return err
	}
	c.sendSize = SOCKET_BUF_SIZE * maxClients
	c.recvSize = SOCKET_BUF_SIZE * maxClients
	c.create()
	return nil
}

// setup xmit buffer pool, socket read/write sizes and kick off readloop
func (c *NetcodeConn) create() {
	c.isClosed = false
	c.xmitBuf.New = func() interface{} {
		return make([]byte, c.maxBytes)
	}
	c.conn.SetReadBuffer(c.recvSize)
	c.conn.SetWriteBuffer(c.sendSize)
	go c.readLoop()
}

// read blocks, so this must be called from a go routine
func (c *NetcodeConn) receiver(ch chan *netcodeData) {
	for {
		if err := c.read(); err == nil {
			select {
			case <-c.closeCh:
				return
			default:
			}
		} else {
			log.Printf("error reading data from socket: %s\n", err)
		}

	}
}

// read does the actual connection read call, verifies we have a
// buffer > 0 and < maxBytes before we bother to attempt to actually
// dispatch it to the recvHandlerFn.
func (c *NetcodeConn) read() error {
	var n int
	var from *net.UDPAddr
	var err error

	data := c.xmitBuf.Get().([]byte)[:c.maxBytes]
	n, from, err = c.conn.ReadFromUDP(data)
	if err != nil {
		return err
	}

	if n == 0 {
		return errors.New("socket error: 0 byte length recv'd")
	}

	if n > c.maxBytes {
		return errors.New("packet size was > maxBytes")
	}
	netData := &netcodeData{}
	netData.data = data[:n]
	netData.from = from
	go c.recvHandlerFn(netData)
	return nil
}

// dispatch the netcodeData to the bound recvHandler function.
func (c *NetcodeConn) readLoop() {
	dataCh := make(chan *netcodeData)
	c.receiver(dataCh)
	<-c.closeCh
}

func main() {
	flag.Parse()
	buf := make([]byte, MAX_PACKET_BYTES)
	for i := 0; i < len(buf); i += 1 {
		buf[i] = byte(i)
	}

	if runProfile {
		p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)
		defer p.Stop()
	}

	if serverMode {
		runServer(buf)
		return
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < maxClients; i += 1 {
		wg.Add(1)
		go runClient(wg, buf, i)
	}
	wg.Wait()
	log.Printf("client total send/recv: %d/%d\n", clientTotalSend, clientTotalRecv)
}

func runServer(buf []byte) {
	conn := NewNetcodeConn()
	recvCount := make([]uint64, maxClients)

	// set our recv handler to just get client ids, increment and spew a buffer back to client
	conn.SetRecvHandler(func(data *netcodeData) {
		// obviously this is dumb and you'd never use userinput to index into a slice, but, testing.
		clientId := binary.LittleEndian.Uint16(data.data)
		atomic.AddUint64(&recvCount[clientId], 1)
		conn.WriteTo(buf, data.from)
	})

	if err := conn.Listen(addr); err != nil {
		log.Fatalf("error in listen: %s\n", err)
	}

	log.Printf("listening on %s\n", addr.String())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// wait for the good ol' ctrl+c
	<-c
	total := uint64(0)
	for i := 0; i < maxClients; i += 1 {
		log.Printf("clientId: %d recv'd/sent %d", i, recvCount[i])
		total += recvCount[i]
	}
	log.Printf("\ntotal: %d\n", total)
	conn.Close()
}

// run our client, sending packets at 10z
func runClient(wg *sync.WaitGroup, buf []byte, index int) {
	clientBuf := make([]byte, len(buf))
	copy(clientBuf, buf)
	binary.LittleEndian.PutUint16(clientBuf[:2], uint16(index))

	doneTimer := time.NewTimer(time.Duration(runTime * float64(time.Second)))
	sendPacket := time.NewTicker(100 * time.Millisecond) // 10hz
	sendCount := uint64(0)
	recvCount := uint64(0)

	conn := NewNetcodeConn()
	conn.SetRecvHandler(func(data *netcodeData) {
		atomic.AddUint64(&recvCount, 1)
	})

	// randomize start up of clients
	if randomizeStart {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}

	if err := conn.Dial(addr); err != nil {
		log.Fatalf("error connecting to %s\n", err)
	}

	for {
		select {
		// time to send the packets!
		case <-sendPacket.C:
			if _, err := conn.Write(clientBuf); err != nil {
				log.Fatalf("error sending packets: %s\n", err)
			}
			atomic.AddUint64(&sendCount, 1)
		case <-doneTimer.C:
			sendPacket.Stop()
			doneTimer.Stop()
			time.Sleep(500 * time.Millisecond)
			rxcnt := atomic.LoadUint64(&recvCount)
			txcnt := atomic.LoadUint64(&sendCount)
			log.Printf("client: %d recv'd: %d sent: %d\n", index, rxcnt, txcnt)
			atomic.AddUint64(&clientTotalRecv, rxcnt)
			atomic.AddUint64(&clientTotalSend, txcnt)
			wg.Done()
			return
		}
	}
}
