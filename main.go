package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"os"
)

var (
	SkipRDB       bool
	Replace       bool
	Path          string
	counter       uint64
	proxyPort     int
	proxyHost     string
	proxyPassword string
)

func getConn(addr string, auth string) (redis.Conn, error) {
	if auth != "" {
		return redis.Dial("tcp", addr, redis.DialPassword(auth))
	}

	return redis.Dial("tcp", addr)
}

func main() {

	flag.StringVar(&Path, "path", "./bloom_filter.rdb", "rdb file path")
	flag.BoolVar(&SkipRDB, "skip-rdb", false, "skip doing command")
	flag.BoolVar(&Replace, "replace", true, "use restore command with replace")
	flag.StringVar(&proxyHost, "proxy-host", "", "Proxy listening interface, default is on all interfaces")
	flag.IntVar(&proxyPort, "proxy-port", 6380, "Proxy port for listening")
	flag.StringVar(&proxyPassword, "proxy-password", "", "Proxy password")
	flag.Parse()

	ch1 := make(chan *RedisCommand, 10)
	chs := []chan *RedisCommand{ch1}

	var result string
	if fileObj, err := os.Open(Path); err == nil {
		defer fileObj.Close()
		contents, _ := io.ReadAll(fileObj)
		result = string(contents)
	}

	go func() {
		err := ParseRDB(bufio.NewReader(bytes.NewBufferString(result)), chs[0], &counter)

		if err != nil {

			// todo handle error
			panic(err)
		}

		close(ch1)

	}()

	conn, err := getConn(fmt.Sprintf("%s:%d", proxyHost, proxyPort), proxyPassword)
	if err != nil {
		panic(err)
	}
	defer func(conn redis.Conn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	for cmd := range ch1 {
		args := make([]interface{}, len(cmd.Command[1:]))

		for i, arg := range cmd.Command[1:] {
			args[i] = arg
		}

		// todo pipeline send
		err := conn.Send(cmd.Command[0], args...)
		if err != nil {
			panic(err)
		}
		err = conn.Flush()
		if err != nil {
			panic(err)
		}

	}

}
