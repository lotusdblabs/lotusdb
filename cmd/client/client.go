package main

import (
	"errors"
	"fmt"
	"github.com/peterh/liner"
	"net"
	"os"
	"strings"
	"time"
)

const cmdHistoryPath = "/tmp/lotus-cli"

// all supported commands.
var commandList = [][]string{
	{"PUT", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"DELETE", "key", "STRING"},
}

var commandSet map[string]bool

var (
	ErrCmdNotFound = errors.New("the command not found")
)

func init() {
	commandSet = make(map[string]bool)
	for _, cmd := range commandList {
		commandSet[strings.ToLower(cmd[0])] = true
	}
}

func main() {
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	line.SetCompleter(func(line string) (c []string) {
		for _, cmd := range commandList {
			if strings.HasPrefix(cmd[0], strings.ToUpper(line)) {
				c = append(c, strings.ToUpper(cmd[0]))
			}
		}
		return
	})

	if f, err := os.Open(cmdHistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	defer func() {
		if f, err := os.Create(cmdHistoryPath); err != nil {
			fmt.Println("Error writing history file: ", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	prompt := "lotusdb>"

	for {
		if cmd, err := line.Prompt(prompt); err == nil {

			if cmd == "quit" {
				fmt.Println("bye")
				break
			} else if cmd == "help" {

			} else {
				cmd = strings.TrimSpace(cmd)
				if err = handleCmd(cmd); err != nil {
					fmt.Println(err)
					continue
				}

				var result string
				result, err = sendCmd(cmd)
				if err != nil {
					fmt.Println(err)
					continue
				}

				fmt.Println(result)
				line.AppendHistory(cmd)
			}

		} else if err == liner.ErrPromptAborted {
			fmt.Println("bye")
			break
		} else {
			fmt.Println("Error reading line: ", err)
			break
		}
	}

}

func handleCmd(cmd string) (err error) {
	ars := strings.Split(cmd, " ")

	if !commandSet[strings.ToLower(ars[0])] {
		err = ErrCmdNotFound
	}

	return
}

func sendCmd(cmd string) (string, error) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:8080", time.Second)
	if err != nil {
		return "", err
	}

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		return "", err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}
