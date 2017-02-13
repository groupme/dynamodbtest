package dynamodbtest

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
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

func read(mpath string) (*os.File, error) {
	f, err := os.OpenFile(mpath, os.O_RDONLY, 0444)
	if err != nil {
		return f, err
	}
	return f, nil
}

func overwrite(mpath string) (*os.File, error) {
	f, err := os.OpenFile(mpath, os.O_RDWR|os.O_TRUNC, 0777)
	if err != nil {
		f, err = os.Create(mpath)
		if err != nil {
			return f, err
		}
	}
	return f, nil
}

func untarIt(basepath, mpath string) {
	fr, err := read(mpath)
	defer fr.Close()
	if err != nil {
		panic(err)
	}
	gr, err := gzip.NewReader(fr)
	defer gr.Close()
	if err != nil {
		panic(err)
	}
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			panic(err)
		}
		path := hdr.Name
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(basepath+path, os.FileMode(hdr.Mode)); err != nil {
				panic(err)
			}
		case tar.TypeReg:
			ow, err := overwrite(basepath + path)
			defer ow.Close()
			if err != nil {
				panic(err)
			}
			if _, err := io.Copy(ow, tr); err != nil {
				panic(err)
			}
		default:
			fmt.Printf("Can't: %c, %s\n", hdr.Typeflag, path)
		}
	}
}

// New returns a started DynamoDB local instance
func New() (*DB, error) {
	port := newPort()
	addr := fmt.Sprintf("localhost:%d", port)
	// if $GOPATH is composed of multiple paths, use the first one (fix for godep)
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		return nil, ErrGopath
	}
	path := gopath + "/src/github.com/miltador/dynamodbtest/"
	archivePath := path + "dynamodb_local_latest.tar.gz"
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		response, err := http.Get("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz")
		if err != nil {
			panic(err)
		}
		defer response.Body.Close()

		f, err := os.Create(archivePath)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		io.Copy(f, response.Body)
	}
	if _, err := os.Stat(path + "DynamoDbLocal_lib/"); os.IsNotExist(err) {
		untarIt(path, archivePath)
	}

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
		for range ticker.C {
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
	return db.cmd.Process.Kill()
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
