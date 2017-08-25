package dynamodbtest

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ConnectTimeout    = 10 * time.Second
	ErrConnectTimeout = errors.New("[dynamodbtest] timeout starting server")
	ErrGopath         = errors.New("[dynamodbtest] GOPATH must be set")
)

// LogOutput must be set before calling New()
var LogOutput bool

// DB represents a DynamoDB Local process
type DB struct {
	addr string
	cmd  *exec.Cmd
}

// New returns a started DynamoDB local instance
func New() (*DB, error) {
	port := newPort()
	addr := fmt.Sprintf("localhost:%d", port)
	// if $GOPATH is composed of multiple paths, use the first one (fix for godep)
	gopath := strings.Split(os.Getenv("GOPATH"), ":")[0]
	if gopath == "" {
		return nil, ErrGopath
	}
	path := gopath + "/src/github.com/groupme/dynamodbtest/"
	db := &DB{
		addr: addr,
		cmd: exec.Command(
			"java",
			"-Djava.library.path="+path+"DynamoDbLocal_lib",
			"-jar",
			path+"DynamoDBLocal.jar",
			"-port",
			fmt.Sprintf("%d", port),
			"-inMemory",
		),
	}

	// log output
	if LogOutput {
		cmdReader, err := db.cmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		scanner := bufio.NewScanner(cmdReader)
		go func() {
			for scanner.Scan() {
				log.Printf("[dynamodbtest:%d] %s\n", port, scanner.Text())
			}
		}()
	}

	// start command
	err := db.cmd.Start()
	if err != nil {
		return nil, err
	}

	// try to connect
	connected := make(chan bool)
	go func() {
		// periodically check if connectable
		ticker := time.NewTicker(time.Millisecond * 10)
		for _ = range ticker.C {
			c, err := net.Dial("tcp", db.addr)
			if c != nil {
				c.Close()
			}
			if err == nil {
				connected <- true
				return
			}
		}
	}()
	select {
	case <-connected:
		return db, nil
	case <-time.After(ConnectTimeout):
		db.Close()
		return nil, ErrConnectTimeout
	}
}

func (db *DB) Close() error {
	err := db.cmd.Process.Kill()
	fmt.Println("Killing dynamodb pid=", db.cmd.Process.Pid, " err=", err)
	_, err = db.cmd.Process.Wait()
	return err
}

func (db *DB) URL() string {
	return fmt.Sprint("http://", db.addr)
}

var portCount int64

func newPort() int {
	port := 8000 + portCount
	atomic.AddInt64(&portCount, 1)
	return int(port)
}
