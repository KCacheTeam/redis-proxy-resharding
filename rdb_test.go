package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"os"
	"testing"
)

func coreTest(t *testing.T, path string) {
	ch1 := make(chan *RedisCommand, 10)
	chs := []chan *RedisCommand{ch1}
	var xxx uint64
	var result string
	if fileObj, err := os.Open(path); err == nil {
		defer fileObj.Close()
		contents, _ := io.ReadAll(fileObj)
		result = string(contents)
	}

	conn, err := getConn("127.0.0.1:6379", "123")
	if err != nil {
		panic(err)
	}
	defer func(conn redis.Conn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	go func() {
		err := ParseRDB(bufio.NewReader(bytes.NewBufferString(result)), chs[0], &xxx)
		if err != nil {
			t.Fatal(err)
		}
		close(ch1)
	}()

	for cmd := range ch1 {
		//abc, _ := hex.DecodeString("00001fb3")
		//if data.Command[1] != string(abc) {
		//	//fmt.Println(hex.EncodeToString([]byte(data.Command[1])))
		//	//continue
		//} else {
		//	//fmt.Println(hex.EncodeToString([]byte(data.Command[1])))
		//}
		//originalBytes := []byte(data.Command[3])
		//encodedStr := hex.EncodeToString(originalBytes)
		//fmt.Println(encodedStr)
		args := make([]interface{}, len(cmd.Command[1:]))

		for i, arg := range cmd.Command[1:] {
			args[i] = arg
		}
		err := conn.Send(cmd.Command[0], args...)
		if err != nil {
			panic(err)
		}
		err = conn.Flush()
		if err != nil {
			panic(err)
		}

	}

	fmt.Println("success pass:" + path)
}

func Test_easily_compressible_string_key(t *testing.T) {
	path := "./cases/easily_compressible_string_key.rdb"
	coreTest(t, path)
}

func Test_hash(t *testing.T) {
	path := "./cases/hash.rdb"
	coreTest(t, path)
}

func Test_hash_as_ziplist(t *testing.T) {
	path := "./cases/hash_as_ziplist.rdb"
	coreTest(t, path)
}
func Test_integer_keys(t *testing.T) {
	path := "./cases/integer_keys.rdb"
	coreTest(t, path)
}
func Test_intset_16(t *testing.T) {
	path := "./cases/intset_16.rdb"
	coreTest(t, path)
}
func Test_intset_32(t *testing.T) {
	path := "./cases/intset_32.rdb"
	coreTest(t, path)
}

func Test_keys_with_expiry(t *testing.T) {
	path := "./cases/keys_with_expiry.rdb"
	coreTest(t, path)
}
func Test_linkedlist(t *testing.T) {
	path := "./cases/linkedlist.rdb"
	coreTest(t, path)
}

func Test_non_ascii_values(t *testing.T) {
	path := "./cases/non_ascii_values.rdb"
	coreTest(t, path)
}

func Test_rdb_version_5_with_checksum(t *testing.T) {
	path := "./cases/rdb_version_5_with_checksum.rdb"
	coreTest(t, path)
}

func Test_regular_set(t *testing.T) {
	path := "./cases/regular_set.rdb"
	coreTest(t, path)
}
func Test_regular_sorted_set(t *testing.T) {
	path := "./cases/regular_sorted_set.rdb"
	coreTest(t, path)
}
func Test_sorted_set_as_ziplist(t *testing.T) {
	path := "./cases/sorted_set_as_ziplist.rdb"
	coreTest(t, path)
}

func Test_ziplist_that_doesnt_compress(t *testing.T) {
	path := "./cases/ziplist_that_doesnt_compress.rdb"
	coreTest(t, path)
}
func Test_empty_database(t *testing.T) {
	path := "./cases/empty_database.rdb"
	coreTest(t, path)
}

func Test_ziplist_with_integers(t *testing.T) {
	path := "./cases/ziplist_with_integers.rdb"
	coreTest(t, path)
}
func Test_zipmap_big_len(t *testing.T) {
	path := "./cases/zipmap_big_len.rdb"
	coreTest(t, path)
}
func Test_zipmap_that_compresses_easily(t *testing.T) {
	path := "./cases/zipmap_that_compresses_easily.rdb"
	coreTest(t, path)
}
func Test_zipmap_that_doesnt_compress(t *testing.T) {
	path := "./cases/zipmap_that_doesnt_compress.rdb"
	coreTest(t, path)
}
func Test_zipmap_with_big_values(t *testing.T) {
	path := "./cases/zipmap_with_big_values.rdb"
	coreTest(t, path)
}
func Test_intset_64(t *testing.T) {
	path := "./cases/intset_64.rdb"
	coreTest(t, path)
}

func Test_uncompressible_string_keys(t *testing.T) {
	path := "./cases/uncompressible_string_keys.rdb"
	coreTest(t, path)
}

func Test_ziplist_that_compresses_easily(t *testing.T) {
	path := "./cases/ziplist_that_compresses_easily.rdb"
	coreTest(t, path)
}

func Test_rdb_version_8_with_64b_length_and_scores(t *testing.T) {
	path := "./cases/rdb_version_8_with_64b_length_and_scores.rdb"
	coreTest(t, path)
}

func Test_BloomFilter(t *testing.T) {
	path := "./cases/bloom_filter.rdb"
	coreTest(t, path)
}

func Test_quicklist(t *testing.T) {
	path := "./cases/quicklist.rdb"
	coreTest(t, path)
}

func Test_parser_filters(t *testing.T) {
	path := "./cases/parser_filters.rdb"
	coreTest(t, path)
}

func Test_memory(t *testing.T) {
	path := "./cases/memory.rdb"
	coreTest(t, path)
}
func Test_tree(t *testing.T) {
	path := "./cases/tree.rdb"
	coreTest(t, path)
}

func Test_cuckoo_filter(t *testing.T) {
	path := "./cases/cuckoo_filter.rdb"
	coreTest(t, path)
}

func Test_topk(t *testing.T) {
	path := "./cases/topk.rdb"
	coreTest(t, path)
}

func Test_t_digest(t *testing.T) {
	path := "./cases/t_digest.rdb"
	coreTest(t, path)
}

func Test_cms(t *testing.T) {
	path := "./cases/cms.rdb"
	coreTest(t, path)
}
