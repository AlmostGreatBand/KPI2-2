package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const activeSuffix = "active"
const segmentPrefix = "segment-"
const maxSize = 10 * 1024 * 1024

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type segment struct {
	path   string
	offset int64
	index  hashIndex
}

type Db struct {
	mux      *sync.Mutex
	out      *os.File
	dir      string
	segments []*segment
}

func NewDb(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, segmentPrefix+activeSuffix)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	var segments []*segment
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range files {
		if strings.HasPrefix(fileInfo.Name(), segmentPrefix) {
			s := &segment{
				path:  filepath.Join(dir, fileInfo.Name()),
				index: make(hashIndex),
			}

			err := s.recover()
			if err != io.EOF {
				return nil, err
			}

			segments = append(segments, s)
		}
	}

	// sort segments
	sort.Slice(segments, func(i, j int) bool {
		stringSuffixI := segments[i].path[len(dir+segmentPrefix)+1:]
		stringSuffixJ := segments[j].path[len(dir+segmentPrefix)+1:]
		if stringSuffixI == activeSuffix {
			return true
		}
		if stringSuffixJ == activeSuffix {
			return false
		}

		suffixI, errI := strconv.Atoi(stringSuffixI)
		suffixJ, errJ := strconv.Atoi(stringSuffixJ)

		return errJ != nil || (errI == nil && suffixI < suffixJ)
	})

	db := &Db{
		mux:      new(sync.Mutex),
		out:      f,
		dir:      dir,
		segments: segments,
	}

	return db, nil
}

const bufSize = 8192

func (s *segment) recover() error {
	input, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)

	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			s.index[e.key] = s.offset
			s.offset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	var (
		position int64
		ok       bool
		s        *segment
	)

	for _, segment := range db.segments {
		if position, ok = segment.index[key]; ok {
			s = segment
			break
		}
	}

	if !ok || s == nil {
		return "", ErrNotFound
	}

	file, err := os.Open(s.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	e := (&entry{
		key:   key,
		value: value,
	}).Encode()

	activeSegment := db.segments[0]

	fi, err := os.Stat(activeSegment.path)
	if err != nil {
		return err
	}

	if fi.Size()+int64(len(e)) >= maxSize {
		activeSegment, err = db.addSegment()
		if err != nil {
			return err
		}
	}

	n, err := db.out.Write(e)
	if err == nil {
		activeSegment.index[key] = activeSegment.offset
		activeSegment.offset += int64(n)
	}
	return err
}

func (db *Db) addSegment() (*segment, error) {
	err := db.out.Close()
	if err != nil {
		return nil, err
	}
	outputPath := filepath.Join(db.dir, segmentPrefix+activeSuffix)
	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%v%v", segmentPrefix, len(db.segments)))
	err = os.Rename(outputPath, segmentPath)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db.out = f

	s := &segment{
		path:  outputPath,
		index: make(hashIndex),
	}
	db.segments = append([]*segment{s}, db.segments...)

	return s, nil
}
