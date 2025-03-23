// Server-impl.go
package srv_client

import (
	"bufio"
	"bytes"
	"fmt"
	"log" // This package is used for logging and has nothing to do with the logic of the program...
	"net"
	"os" // This package is used for logging and has nothing to do with the logic of the program...
	"strconv"

	"srv_client/srv_client/kvstore"
)

// -------------------------------Log Section-----------------------------------------------------------
const BLACK = "\033[0;30m"
const RED = "\033[0;31m"
const GREEN = "\033[0;32m"
const YELLOW = "\033[0;33m"
const BLUE = "\033[0;34m"
const MAGENTA = "\033[0;35m"
const CYAN = "\033[0;36m"
const WHITE = "\033[0;37m"
const RESET = "\033[0m"

type Logger struct {
	// mu           sync.Mutex
	logger       *log.Logger
	debugEnabled bool
	flag         bool // On-Off switch for the logger
}

func NewLogger(debug bool, flag bool) *Logger {
	return &Logger{
		logger:       log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
		debugEnabled: debug,
		flag:         flag,
	}
}

func (l *Logger) logInternal(color, level, format string, v ...interface{}) {
	if color == "" {
		color = RESET
	}

	message := fmt.Sprintf(format, v...)
	logLine := fmt.Sprintf("%s[%s] %s%s", color, level, message, RESET)

	// l.mu.Lock()
	// defer l.mu.Unlock()

	l.logger.Output(3, logLine)
}

func (l *Logger) Info(color, format string, v ...interface{}) {
	if l.flag {
		l.logInternal(color, "INFO", format, v...)
	}
}

var lgr *Logger

func InitGlobalLogger(debug bool, flag bool) {
	lgr = NewLogger(debug, flag)
}

// -------------------------------End of Log Section-----------------------------------------------------------

// -------------------------------Declarations--------------------------------------------
type server struct {
	store          kvstore.KVStore
	listener       net.Listener
	eventChan      chan interface{}
	queryChan      chan queryRequest
	shutdown       chan struct{}
	clients        map[*client]bool
	activeClients  int
	droppedClients int
}

type client struct {
	conn net.Conn
	out  chan []byte
}

type clientConnect struct {
	client *client
}

type clientDisconnect struct {
	client *client
}

type clientCmd struct {
	client *client
	line   []byte
}

type queryRequest struct {
	queryType string
	reply     chan int
}

// -------------------------------End of Declarations--------------------------------------------

func New(store kvstore.KVStore) KeyValueServer {
	s := &server{
		store:          store,
		eventChan:      make(chan interface{}, 10), // buffer capacity of 100 events
		queryChan:      make(chan queryRequest),
		shutdown:       make(chan struct{}),
		clients:        make(map[*client]bool),
		activeClients:  0,
		droppedClients: 0,
	}
	return s
}

func (s *server) Start(port int) error {
	InitGlobalLogger(false, true)
	if s.listener != nil {
		return fmt.Errorf("server already started or closed")
	}
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	s.listener = ln
	lgr.Info(CYAN, "Started the server successfully on port %d", port)

	go s.manager()
	go s.acceptLoop()
	lgr.Info(GREEN, "Successfully started accept loop and manager routines.")
	return nil
}

func (s *server) CountActive() int {
	reply := make(chan int)
	s.queryChan <- queryRequest{queryType: "active", reply: reply}
	return <-reply
}

func (s *server) CountDropped() int {
	reply := make(chan int)
	s.queryChan <- queryRequest{queryType: "dropped", reply: reply}
	return <-reply
}

func (s *server) Close() {
	lgr.Info(YELLOW, "Shutting down the server...")
	close(s.shutdown)
	if s.listener != nil {
		s.listener.Close()
	}
	lgr.Info(BLUE, "Server closed successfully.")
}

// ---------------------------------------------------------------------------

func (s *server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				lgr.Info(YELLOW, "Shutdown signal received...")
				return
			default:
				return
			}
		}
		c := &client{
			conn: conn,
			out:  make(chan []byte, 100),
		}
		lgr.Info(MAGENTA, "New client accepted: %s", conn.RemoteAddr())
		s.eventChan <- clientConnect{client: c}
		go s.handleClientRead(c)
		go s.handleClientWrite(c)
		lgr.Info(MAGENTA, "Client %s handed over Read and Write routines.", conn.RemoteAddr())
	}
}

func (s *server) handleClientRead(c *client) {
	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			lgr.Info(RED, "Error on reading from client %s: %v", c.conn.RemoteAddr(), err)
			break
		}
		data := make([]byte, len(line))
		copy(data, line)
		s.eventChan <- clientCmd{client: c, line: data}
		lgr.Info(CYAN, "Recieved message from client %s: %s", c.conn.RemoteAddr(), line)
	}
	s.eventChan <- clientDisconnect{client: c}
	c.conn.Close()
}

func (s *server) handleClientWrite(c *client) {
	for {
		select {
		case msg, ok := <-c.out:
			if !ok {
				return
			}
			_, err := c.conn.Write(msg)
			if err != nil {
				return
			}
		case <-s.shutdown:
			return
		}
	}
}

func (s *server) manager() {
	for {
		select {
		case ev := <-s.eventChan:
			switch e := ev.(type) {
			case clientConnect:
				s.clients[e.client] = true
				s.activeClients++
				// lgr.Info(YELLOW, "Client %s is connecting.", )
			case clientDisconnect:
				if _, ok := s.clients[e.client]; ok {
					delete(s.clients, e.client)
					s.activeClients--
					s.droppedClients++
					close(e.client.out)
					e.client.conn.Close()
				}
			case clientCmd:
				s.handleCommand(e.client, e.line)
			}
		case req := <-s.queryChan:
			if req.queryType == "active" {
				req.reply <- s.activeClients
			} else if req.queryType == "dropped" {
				req.reply <- s.droppedClients
			}
		case <-s.shutdown:
			for cli := range s.clients {
				cli.conn.Close()
				close(cli.out)
			}
			return
		}
	}
}

func (s *server) handleCommand(c *client, line []byte) {
	trimmed := bytes.TrimRight(line, "\n")
	parts := bytes.Split(trimmed, []byte(":"))
	if len(parts) < 2 {
		lgr.Info(YELLOW, "Recieved a malformed command from %s. Ignoring the input...", c.conn.RemoteAddr())
		return
	}
	cmd := string(parts[0])
	switch cmd {
	case "Put":
		if len(parts) != 3 {
			lgr.Info(YELLOW, "Recieved a malformed PUT command from %s. Ignoring the input...", c.conn.RemoteAddr())
			return
		}
		key := string(parts[1])
		value := parts[2]
		s.store.Put(key, value)
		lgr.Info(CYAN, "PUT command from %s processed.", c.conn.RemoteAddr())

	case "Get":
		if len(parts) != 2 {
			lgr.Info(YELLOW, "Recieved a malformed GET command from %s. Ignoring the input...", c.conn.RemoteAddr())
			return
		}
		key := string(parts[1])
		values := s.store.Get(key)
		for _, val := range values {
			resp := []byte(key + ":" + string(val))
			if len(resp) == 0 || resp[len(resp)-1] != '\n' {
				resp = append(resp, '\n')
			}
			select {
			case c.out <- resp:
			default:
				lgr.Info(YELLOW, "Client %s output buffer is full. Dropping the message", c.conn.RemoteAddr())
			}
		}
		lgr.Info(CYAN, "GET command from %s processed.", c.conn.RemoteAddr())

	case "Delete":
		if len(parts) != 2 {
			lgr.Info(YELLOW, "Recieved a malformed DELETE command from %s. Ignoring the input...", c.conn.RemoteAddr())
			return
		}
		key := string(parts[1])
		s.store.Delete(key)
		lgr.Info(CYAN, "DELETE command from %s processed.", c.conn.RemoteAddr())

	case "Update":
		if len(parts) != 4 {
			lgr.Info(YELLOW, "Recieved a malformed UPDATE command from %s. Ignoring the input...", c.conn.RemoteAddr())
			return
		}
		key := string(parts[1])
		oldVal := parts[2]
		newVal := parts[3]
		s.store.Update(key, oldVal, newVal)
		lgr.Info(CYAN, "UPDATE command from %s processed.", c.conn.RemoteAddr())

	default:
		lgr.Info(YELLOW, "Recieved an unknown command from %s. Ignoring the input...", c.conn.RemoteAddr())
	}
}
