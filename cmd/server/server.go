package main

import (
	"errors"
	"fmt"
	"github.com/flowercorp/lotusdb"
	"log"
	"net"
	"strings"
	"time"
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
)

var db *lotusdb.LotusDB

var (
	ErrArgsNumNotMatch = errors.New("the number of args not match")
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
	listener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		panic(err)
	}
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
	fmt.Println("   _       ___    _____   _   _    ___     ___     ___   \n  | |     / _ \\  |_   _| | | | |  / __|   |   \\   | _ )  \n  | |__  | (_) |   | |   | |_| |  \\__ \\   | |) |  | _ \\  \n  |____|  \\___/   _|_|_   \\___/   |___/   |___/   |___/  ")
}

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
