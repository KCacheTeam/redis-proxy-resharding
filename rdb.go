package main

// Parser RDB file per spec: https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
)

const (
	rdbOpAux        = 0xFA
	rdbOpResizeDB   = 0xFB
	rdbOpDB         = 0xFE
	rdbOpExpirySec  = 0xFD
	rdbOpExpiryMSec = 0xFC
	rdbOpEOF        = 0xFF

	rdbLen6Bit  = 0x0
	rdbLen14bit = 0x1
	rdbLen32Bit = 0x2
	rdbLenEnc   = 0x3

	Type32Bit = 0x80
	Type64Bit = 0x81

	rdbOpString          = 0x00
	rdbOpList            = 0x01
	rdbOpSet             = 0x02
	rdbOpZset            = 0x03
	rdbOpHash            = 0x04
	rdbOpZset2           = 0x05
	rdbOpModule2         = 0x07
	rdbOpZipmap          = 0x09
	rdbOpZiplist         = 0x0a
	rdbOpIntset          = 0x0b
	rdbOpSortedSet       = 0x0c
	rdbOpHashmap         = 0x0d
	rdbOpListQuicklist   = 0x0e
	rdbOpStreamListpacks = 0x0f

	RdbModuleOpcodeEOF    = 0
	RdbModuleOpcodeSInt   = 1
	RdbModuleOpcodeUInt   = 2
	RdbModuleOpcodeFloat  = 3
	RdbModuleOpcodeDouble = 4
	RdbModuleOpcodeString = 5
)

var (
	rdbSignature     = []byte{0x52, 0x45, 0x44, 0x49, 0x53}
	restoreCommand   = "RESTORE"
	currentTimestamp = uint64(0)
)

var (
	// ErrWrongSignature is returned when RDB signature can't be parsed
	ErrWrongSignature = errors.New("rdb: wrong signature")
	// ErrVersionUnsupported is returned when RDB version is too high (can't parse)
	ErrVersionUnsupported = errors.New("rdb: version unsupported")
	// ErrUnsupportedOp is returned when unsupported operation is encountered in RDB
	ErrUnsupportedOp = errors.New("rdb: unsupported opcode")
	// ErrUnsupportedStringEnc is returned when unsupported string encoding is encountered in RDB
	ErrUnsupportedStringEnc = errors.New("rdb: unsupported string encoding")
)

type RedisCommand struct {
	Command  []string
	BulkSize int64
}

// Parser holds internal state of RDB parser while running
type Parser struct {
	reader *bufio.Reader
	output chan *RedisCommand

	length int64
	hash   uint64

	rawData []byte
	key     string
	expiry  uint64

	counter *uint64

	rdbVersion      int
	rdbVersion16bit []byte
	valueState      state
	currentOp       byte
}

type state func(parser *Parser) (nextstate state, err error)

// ParseRDB parsers RDB file which is read from reader, sending chunks of data through output channel
// length is original length of RDB file
func ParseRDB(reader *bufio.Reader, output chan *RedisCommand, counter *uint64) (err error) {
	currentTimestamp = uint64(time.Now().Unix())
	go cron()

	parser := &Parser{
		reader:  reader,
		output:  output,
		counter: counter,
	}

	state := stateMagic

	for state != nil {
		state, err = state(parser)
		if err != nil {
			return
		}
	}

	return nil
}

func cron() {
	ticker := time.NewTicker(time.Second * 1)

	for {
		select {
		case <-ticker.C:
			currentTimestamp = uint64(time.Now().Unix())
		}
	}
}

// Read exactly n bytes
func (parser *Parser) safeRead(n uint64) (result []byte, err error) {
	result = make([]byte, n)
	_, err = io.ReadFull(parser.reader, result)
	return
}

// Accumulate some data that might be either parsered out or passed through
func (parser *Parser) commandWrite(save bool, data []byte) {
	if !save {
		return
	}

	if parser.rawData == nil {
		parser.rawData = make([]byte, len(data), 4096)
		copy(parser.rawData, data)
	} else {
		parser.rawData = append(parser.rawData, data...)
	}

}

func (parser *Parser) appendVersion() {
	parser.rawData = append(parser.rawData, parser.rdbVersion16bit...)
}

func (parser *Parser) buildCRCData() {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, CRC64Update(0, parser.rawData))
	parser.rawData = append(parser.rawData, buf...)
}
func toHex(data []byte) string {
	//	result := ""
	result := "\""
	for _, b := range data {
		result = result + fmt.Sprintf("\\x%02x", b)
	}
	result += "\""
	// result := fmt.Sprintf("%x", data)
	return result
}

// Discard or keep saved data
func (parser *Parser) keep() {
	parser.appendVersion()
	parser.buildCRCData()

	var cmd *RedisCommand
	if Replace {
		cmd = &RedisCommand{
			Command: []string{
				restoreCommand,
				parser.key,
				fmt.Sprint(parser.expiry),
				string(parser.rawData),
				"REPLACE",
			},
		}
	} else {
		cmd = &RedisCommand{
			Command: []string{
				restoreCommand,
				parser.key,
				fmt.Sprint(parser.expiry),
				string(parser.rawData),
			},
		}
	}

	if !SkipRDB {
		parser.output <- cmd

		if parser.counter != nil {
			(*parser.counter)++
		}
	}

	parser.rawData = []byte{}
	parser.expiry = 0
}

// Read length encoded prefix
func (parser *Parser) readLength(save bool) (length uint64, encoding int8, err error) {
	prefix, err := parser.reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	parser.commandWrite(save, []byte{prefix})

	kind := (prefix & 0xC0) >> 6

	switch kind {
	case rdbLen6Bit: // 0x00
		length = uint64(prefix & 0x3F)
		return length, -1, nil
	case rdbLen14bit: // 0x01
		data, err := parser.reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		parser.commandWrite(save, []byte{data})
		length = ((uint64(prefix) & 0x3F) << 8) | uint64(data)
		return length, -1, nil
	case rdbLen32Bit: // 0x10
		if prefix == Type32Bit {
			data, err := parser.safeRead(4)
			if err != nil {
				return 0, 0, err
			}
			parser.commandWrite(save, data)
			length = uint64(binary.BigEndian.Uint32(data))
			return length, -1, nil
		} else if prefix == Type64Bit {
			data, err := parser.safeRead(8)
			if err != nil {
				return 0, 0, err
			}
			parser.commandWrite(save, data)
			length = binary.BigEndian.Uint64(data)
			return length, -1, nil
		}

	case rdbLenEnc: // wtf
		encoding = int8(prefix & 0x3F)
		return 0, encoding, nil
	}
	panic("never reached")
}

// read string from RDB, only uncompressed version is supported
func (parser *Parser) readString(save bool) (string, error) {
	var result string

	length, encoding, err := parser.readLength(false)

	if err != nil {
		return "", err
	}

	switch encoding {
	// length-prefixed string
	case -1:
		data, err := parser.safeRead(length)
		if err != nil {
			return "", err
		}
		parser.commandWrite(save, data)
		result = string(data)
		// integer as string
	case 0, 1, 2:
		data, err := parser.safeRead(1 << uint8(encoding))

		if err != nil {
			return "", err
		}
		parser.commandWrite(save, data)

		var num uint32

		if encoding == 0 {
			num = uint32(data[0])
		} else if encoding == 1 {
			num = uint32(data[0]) | (uint32(data[1]) << 8)
		} else if encoding == 2 {
			num = uint32(data[0]) | (uint32(data[1]) << 8) | (uint32(data[2]) << 16) | (uint32(data[3]) << 24)
		}

		result = fmt.Sprintf("%d", num)
		// compressed string
	case 3:
		clength, _, err := parser.readLength(save)
		if err != nil {
			return "", err
		}
		length, _, err := parser.readLength(save)
		if err != nil {
			return "", err
		}
		data, err := parser.safeRead(clength)

		if err != nil {
			return "", err
		}

		parser.commandWrite(save, data)

		result = string(lzfDecompressNew(data, int(clength), int(length)))
	default:
		return "", ErrUnsupportedStringEnc
	}

	return result, nil
}

// skip (copy) string from RDB
func (parser *Parser) copyString(save bool) error {
	length, encoding, err := parser.readLength(save)
	if err != nil {
		return err
	}

	switch encoding {
	// length-prefixed string
	case -1:
		data, err := parser.safeRead(length)
		if err != nil {
			return err
		}
		parser.commandWrite(save, data)
		// integer as string
	case 0, 1, 2:
		data, err := parser.safeRead(1 << uint8(encoding))
		if err != nil {
			return err
		}
		parser.commandWrite(save, data)
		// compressed string
	case 3:
		clength, _, err := parser.readLength(save)
		if err != nil {
			return err
		}
		_, _, err = parser.readLength(save)
		if err != nil {
			return err
		}
		data, err := parser.safeRead(clength)

		if err != nil {
			return err
		}
		parser.commandWrite(save, data)
	default:
		return ErrUnsupportedStringEnc
	}
	return nil
}

// read RDB magic header
func stateMagic(parser *Parser) (state, error) {
	signature, err := parser.safeRead(5)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(signature, rdbSignature) != 0 {
		return nil, ErrWrongSignature
	}

	versionRaw, err := parser.safeRead(4)
	if err != nil {
		return nil, err
	}
	version, err := strconv.Atoi(string(versionRaw))
	if err != nil {
		return nil, ErrWrongSignature
	}

	if version > 9 {
		return nil, ErrVersionUnsupported
	}

	parser.rdbVersion = version
	parser.rdbVersion16bit = make([]byte, 2)
	binary.LittleEndian.PutUint16(parser.rdbVersion16bit, uint16(version))

	return stateOp, nil
}

// main selector of operations
func stateOp(parser *Parser) (state, error) {
	op, err := parser.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	parser.currentOp = op

	if parser.currentOp != rdbOpDB && parser.currentOp != rdbOpExpirySec && parser.currentOp != rdbOpExpiryMSec &&
		parser.currentOp != rdbOpAux && parser.currentOp != rdbOpResizeDB {
		parser.commandWrite(true, []byte{op})
	}

	switch op {
	case rdbOpDB:
		return stateDB, nil
	case rdbOpExpirySec:
		return stateExpirySec, nil
	case rdbOpExpiryMSec:
		return stateExpiryMSec, nil
	case rdbOpString, rdbOpZipmap, rdbOpIntset, rdbOpZiplist, rdbOpSortedSet, rdbOpHashmap:
		parser.valueState = stateCopyString
		return stateKey, nil
	case rdbOpList, rdbOpSet:
		parser.valueState = stateCopySetOrList
		return stateKey, nil
	case rdbOpZset:
		parser.valueState = stateCopyZset
		return stateKey, nil
	case rdbOpZset2:
		parser.valueState = stateCopyZset2
		return stateKey, nil
	case rdbOpHash:
		parser.valueState = stateCopyHash
		return stateKey, nil
	case rdbOpModule2:
		parser.valueState = stateCopyModule2
		return stateKey, nil
	case rdbOpAux:
		return stateAux, nil
	case rdbOpResizeDB:
		return stateResizeDB, nil
	case rdbOpListQuicklist:
		parser.valueState = stateCopyQuicklist
		return stateKey, nil
	case rdbOpStreamListpacks:
		parser.valueState = stateCopyListpacks
		return stateKey, nil
	case rdbOpEOF:
		if parser.rdbVersion > 4 {
			return stateCRC64, nil
		}
		return statePadding, nil
	default:
		return nil, ErrUnsupportedOp
	}
}

func stateResizeDB(parser *Parser) (state, error) {
	dbSize, _, err := parser.readLength(false)
	if err != nil {
		return nil, err
	}

	expireSize, _, err := parser.readLength(false)
	if err != nil {
		return nil, err
	}
	nouse(dbSize, expireSize)
	// pp.Printf("dbsize %s expireSize %s\n", int(dbSize), int(expireSize))
	return stateOp, nil

}

func stateAux(parser *Parser) (state, error) {
	key, err := parser.readString(false)
	if err != nil {
		return nil, err
	}

	value, err := parser.readString(false)
	if err != nil {
		return nil, err
	}
	// pp.Println(key, value)

	nouse(key, value)
	return stateOp, nil
}

// DB index operation
func stateDB(parser *Parser) (state, error) {
	_, _, err := parser.readLength(false) // skip 0x00
	if err != nil {
		return nil, err
	}

	return stateOp, nil
}

func stateExpirySec(parser *Parser) (state, error) {
	expiry, err := parser.safeRead(4)
	if err != nil {
		return nil, err
	}

	fd := binary.LittleEndian.Uint64(expiry)

	timeGap := fd - currentTimestamp

	if timeGap < 0 {
		parser.expiry = 1
	} else {
		parser.expiry = timeGap * 1000
	}

	return stateOp, nil
}

func stateExpiryMSec(parser *Parser) (state, error) {
	expiry, err := parser.safeRead(8)
	if err != nil {
		return nil, err
	}

	fc := binary.LittleEndian.Uint64(expiry)
	timeGap := fc - currentTimestamp*1000

	if fc < currentTimestamp*1000 {
		parser.expiry = 1
	} else {
		parser.expiry = timeGap
	}

	return stateOp, nil
}

// read key
func stateKey(parser *Parser) (state, error) {
	key, err := parser.readString(false)
	if err != nil {
		return nil, err
	}

	parser.key = key

	return parser.valueState, nil
}

// skip over string
func stateCopyString(parser *Parser) (state, error) {
	err := parser.copyString(true)
	if err != nil {
		return nil, err
	}

	parser.keep()
	return stateOp, nil
}

// skip over set or list
func stateCopySetOrList(parser *Parser) (state, error) {
	length, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}

	var i uint64

	for i = 0; i < length; i++ {
		// list element
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}
	}

	parser.keep()
	return stateOp, nil
}

// skip over hash
func stateCopyHash(parser *Parser) (state, error) {
	length, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}

	var i uint64

	for i = 0; i < length; i++ {
		// key
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}

		// value
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}
	}

	parser.keep()
	return stateOp, nil
}

// skip over zset
func stateCopyZset(parser *Parser) (state, error) {
	length, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}

	var i uint64

	for i = 0; i < length; i++ {
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}

		dlen, err := parser.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		parser.commandWrite(true, []byte{dlen})

		if dlen < 0xFD {
			double, err := parser.safeRead(uint64(dlen))
			if err != nil {
				return nil, err
			}
			parser.commandWrite(true, double)
		}
	}

	parser.keep()
	return stateOp, nil
}

// skip over zset
func stateCopyZset2(parser *Parser) (state, error) {
	length, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}

	var i uint64

	for i = 0; i < length; i++ {
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}

		scoreBytes, err := parser.safeRead(uint64(8))
		if err != nil {
			return nil, err
		}
		// score double
		score := math.Float64frombits(binary.LittleEndian.Uint64(scoreBytes))
		nouse(score)
	}

	parser.keep()
	return stateOp, nil
}
func stateCopyQuicklist(parser *Parser) (state, error) {
	listLen, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}

	for i := uint64(0); i < listLen; i++ {
		err = parser.copyString(true)
		if err != nil {
			return nil, err
		}

	}
	parser.keep()
	return stateOp, nil
}
func stateCopyListpacks(parser *Parser) (state, error) {
	return nil, errors.New("not imp")
}

// re-calculate crc64
func stateCRC64(parser *Parser) (state, error) {
	_, err := parser.safeRead(8)
	if err != nil {
		return nil, err
	}
	return statePadding, nil
}

// pad RDB with 0xFF up to original length
func statePadding(parser *Parser) (state, error) {
	return nil, nil
}

func (parser *Parser) readUnsigned(save bool) (uint64, error) {
	val, _, err := parser.readLength(save)
	if err != nil {
		return 0, err
	} else if val != RdbModuleOpcodeUInt {
		return 0, errors.New(fmt.Sprintf("illegal RdbModuleOpcodeUInt %d,expect:%d", val, RdbModuleOpcodeUInt))
	}

	value, _, err := parser.readLength(save)
	if err != nil {
		return 0, err
	}

	// pp.Printf("type %d value %s\n", opcode, int(value))
	return value, nil
}

func (parser *Parser) readDouble(save bool) (uint32, error) {
	val, _, err := parser.readLength(save)
	if err != nil {
		return 0, err
	} else if val != RdbModuleOpcodeDouble {
		return 0, errors.New(fmt.Sprintf("illegal RdbModuleOpcodeDouble %d,expect:%d", val, RdbModuleOpcodeDouble))
	}

	scoreBytes, err := parser.safeRead(uint64(8))
	if err != nil {
		return 0, err
	}
	parser.commandWrite(true, scoreBytes)
	// score
	math.Float64frombits(binary.LittleEndian.Uint64(scoreBytes))

	return 0, nil
}

func (parser *Parser) readStringBuffer(save bool) (uint32, error) {
	val, _, err := parser.readLength(save)
	if err != nil {
		return 0, err
	} else if val != RdbModuleOpcodeString {
		return 0, errors.New(fmt.Sprintf("illegal RdbModuleOpcodeString %d,expect:%d", val, RdbModuleOpcodeString))
	}

	err = parser.copyString(save)
	if err != nil {
		return 0, err
	}

	// pp.Printf("type %d value %s\n", opcode, int(value))
	return 0, nil
}

func stateCopyModule2(parser *Parser) (state, error) {
	length, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	}
	if length == 3465209449566631940 {
		// bloomFilter
		return stateCopyBloomFilter(parser)
	} else if length == 3465209449562641412 {
		// cuckooFilter
		return stateCopyCuckooFilter(parser)
	} else {
		fmt.Println(length)
		return nil, errors.New("目前只支持布隆过滤器模块")
	}

}

func stateCopyBloomFilter(parser *Parser) (state, error) {
	// size
	_, err := parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// nfilters
	nfilters, err := parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// options
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// growth
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}

	numFilters := int(nfilters)
	i := 0
	for {
		if i >= numFilters {
			break
		}
		i += 1

		// entries
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}

		// error
		_, err = parser.readDouble(true)
		if err != nil {
			return nil, err
		}

		// hashes
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}
		// bpe
		_, err = parser.readDouble(true)
		if err != nil {
			return nil, err
		}

		// bits
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}
		// n2
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}

		// string buffer
		_, err = parser.readStringBuffer(true)
		if err != nil {
			return nil, err
		}
		// size
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}

	}

	eof, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	} else if eof != RdbModuleOpcodeEOF {
		return nil, errors.New(fmt.Sprintf("illegal RdbModuleOpcodeString %d,expect:%d", eof, RdbModuleOpcodeEOF))
	}
	parser.keep()
	return stateOp, nil
}

func stateCopyCuckooFilter(parser *Parser) (state, error) {
	numFilters, err := parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// numBuckets
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// numItems
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// numDeletes
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// bucketSize
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// maxIterations
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}
	// expansion
	_, err = parser.readUnsigned(true)
	if err != nil {
		return nil, err
	}

	i := uint64(0)
	for {
		if i >= numFilters {
			break
		}
		i++
		// filters[i].numBuckets
		_, err = parser.readUnsigned(true)
		if err != nil {
			return nil, err
		}

		// string buffer
		_, err = parser.readStringBuffer(true)
		if err != nil {
			return nil, err
		}

	}

	eof, _, err := parser.readLength(true)
	if err != nil {
		return nil, err
	} else if eof != RdbModuleOpcodeEOF {
		return nil, errors.New(fmt.Sprintf("illegal RdbModuleOpcodeString %d,expect:%d", eof, RdbModuleOpcodeEOF))
	}
	parser.keep()
	return stateOp, nil
}

func nouse(i ...interface{}) {

}
