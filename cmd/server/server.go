package main

import (
	"errors"
	"fmt"
	"github.com/flower-corp/lotusdb"
	"github.com/flower-corp/lotusdb/logger"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	// PUT put command.
	PUT = "PUT"
	// GET get command.
	GET = "GET"
	// DELETE delete command.
	DELETE = "DELETE"
)

var db *lotusdb.LotusDB

var (
	// ErrArgsNumNotMatch number of args not match.
	ErrArgsNumNotMatch = errors.New("the number of args not match")
	// ErrNotSupportedCmd command not be supported.
	ErrNotSupportedCmd = errors.New("the command not be supported")
)

func init() {
	var err error
	options := lotusdb.DefaultOptions("/tmp/lotusdb")
	db, err = lotusdb.Open(options)
	if err != nil {
		panic(err)
	}
}

func main() {
	banner()
	listener, err := net.Listen("tcp", "127.0.0.1:9230")
	if err != nil {
		panic(err)
	}

	logger.Info("lotusdb server is running")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-sig
		os.Exit(-1)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			log.Println(err)
			continue
		}

		cmds := strings.Split(string(buf[:n]), " ")

		var response []byte

		start := time.Now()
		response, err = ExecuteCmds(cmds)
		if err != nil {
			response = []byte("ERROR:" + err.Error())
		} else {
			response = []byte(fmt.Sprintf("%s (%s)", string(response), time.Since(start)))
		}
		conn.Write(response)
	}
}

func banner() {
	b, err := ioutil.ReadFile("../../resource/banner/banner.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
	fmt.Println("A powerful storage engine for TiDB and TiKV")
}

// ExecuteCmds execute command and get result.
func ExecuteCmds(cmds []string) (result []byte, err error) {
	switch strings.ToUpper(cmds[0]) {
	case PUT:
		if err = check(cmds[1:], 2); err != nil {
			return
		}
		err = db.Put([]byte(cmds[1]), []byte(cmds[2]))
	case GET:
		if err = check(cmds[1:], 1); err != nil {
			return
		}
		result, err = db.Get([]byte(cmds[1]))
		if result == nil {
			result = []byte("empty")
		}
	case DELETE:
		if err = check(cmds[1:], 1); err != nil {
			return
		}
		err = db.Delete([]byte(cmds[1]))
	default:
		err = ErrNotSupportedCmd
	}
	return
}

func check(args []string, num int) error {
	if len(args) != num {
		return ErrArgsNumNotMatch
	}
	return nil
}
