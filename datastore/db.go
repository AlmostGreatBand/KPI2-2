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
const mergedSuffix = "merged"
const segmentPrefix = "segment-"
const defMaxActiveSize = 10 * 1024 * 1024

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type segment struct {
	path   string
	offset int64
	index  hashIndex
}

type putEntry struct {
	entry *entry
	responseChan chan error
}

type Db struct {
	mux      *sync.RWMutex
	out      *os.File

	dir              string
	activeBlockSize  int64
	autoMergeEnabled bool

	segments []*segment
	mergeChan chan int
	putChan   chan putEntry
}

func NewDb(dir string) (*Db, error) {
	return NewDbSizedMerge(dir, defMaxActiveSize, true)
}

func NewDbMerge(dir string, autoMergeEnabled bool) (*Db, error) {
	return NewDbSizedMerge(dir, defMaxActiveSize, autoMergeEnabled)
}

func NewDbSized(dir string, activeBlockSize int64) (*Db, error) {
	return NewDbSizedMerge(dir, activeBlockSize, true)
}

func NewDbSizedMerge(dir string, activeBlockSize int64, autoMergeEnabled bool) (*Db, error) {
	outputPath := filepath.Join(dir, segmentPrefix + activeSuffix)
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
		stringSuffixI := segments[i].path[len(dir + segmentPrefix) + 1:]
		stringSuffixJ := segments[j].path[len(dir + segmentPrefix) + 1:]
		if stringSuffixI == activeSuffix || stringSuffixJ == mergedSuffix {
			return true
		}
		if stringSuffixJ == activeSuffix || stringSuffixI == mergedSuffix {
			return false
		}

		suffixI, errI := strconv.Atoi(stringSuffixI)
		suffixJ, errJ := strconv.Atoi(stringSuffixJ)

		return errJ != nil || (errI != nil && suffixI > suffixJ)
	})

	mergeChan := make(chan int)
	putChan := make(chan putEntry)

	db := &Db{
		mux:              new(sync.RWMutex),
		out:              f,
		dir:              dir,
		activeBlockSize:  activeBlockSize,
		autoMergeEnabled: autoMergeEnabled,
		segments:         segments,
		mergeChan:        mergeChan,
		putChan:          putChan,
	}

	go func() {
		for el := range mergeChan {
			if el == 0 {
				return
			}

			db.merge()
		}
	}()

	go func() {
		for el := range putChan {
			if el.entry == nil {
				return
			}

			db.put(el)
		}
	}()

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
	db.mergeChan <- 0
	db.putChan <- putEntry{ entry: nil }
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var (
		value	 string
		err      error
	)

	for _, segment := range db.segments {
		if value, err = segment.get(key); err == nil {
			return value, nil
		}
	}

	return "", err
}

func (s *segment) get(key string) (string, error) {
	var (
		position int64
		ok		 bool
	)

	if position, ok = s.index[key]; !ok {
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
	responseChan := make(chan error)
	e := &entry{ key: key, value: value }

	db.putChan <- putEntry{ entry: e, responseChan: responseChan }
	res := <- responseChan
	return res
}

func (db *Db) put(pe putEntry) {
	db.mux.Lock()
	defer db.mux.Unlock()

	if len(db.segments) > 2 && db.autoMergeEnabled {
		go func() {
			db.mergeChan <- 1
		}()
	}

	e := pe.entry
	n, err := db.out.Write(e.Encode())
	if err != nil {
		pe.responseChan <- err
		return
	}

	activeSegment := db.segments[0]
	activeSegment.index[e.key] = activeSegment.offset
	activeSegment.offset += int64(n)

	fi, err := os.Stat(activeSegment.path)
	if err != nil {
		fmt.Errorf("can not read active file stat: %v", err)
		// return nil because we have already put value to db and user shouldn't know about segmentation error
		pe.responseChan <- nil
		return
	}

	if fi.Size() >= db.activeBlockSize {
		activeSegment, err = db.addSegment()
		if err != nil {
			fmt.Errorf("can not create new segment: %v", err)
			// return nil because we have already put value to db and user shouldn't know about segmentation error
			pe.responseChan <- nil
			return
		}
	}

	pe.responseChan <- nil
}

func (db *Db) addSegment() (*segment, error) {
	err := db.out.Close()
	if err != nil {
		return nil, err
	}

	outputPath := filepath.Join(db.dir, segmentPrefix + activeSuffix)
	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%v%v", segmentPrefix, len(db.segments) - 1))
	err = os.Rename(outputPath, segmentPath)
	if err != nil {
		return nil, err
	}
	db.segments[0].path = segmentPath

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

func (db *Db) merge() {
	segmentsToMerge := db.segments[1:]
	segments := make([]*segment, len(segmentsToMerge))
	copy(segments, segmentsToMerge)

	if len(segments) < 2 {
		return
	}

	keysSegments := make(map[string]*segment)

	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		for k,_ := range segments[i].index {
			keysSegments[k] = s
		}
	}

	segmentPath := filepath.Join(db.dir, segmentPrefix + mergedSuffix)
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		fmt.Errorf("error occured in merge: %v", err)
		return
	}
	defer f.Close()

	segment := &segment{
		path: segmentPath,
		index: make(hashIndex),
	}

	for k, s := range keysSegments {
		value, err := s.get(k)
		e := (&entry{
			key:   k,
			value: value,
		}).Encode()

		n, err := f.Write(e)
		if err != nil {
			fmt.Errorf("error occured in merge: %v", err)
			return
		}
		segment.index[k] = segment.offset
		segment.offset += int64(n)
	}

	db.mux.Lock()
	to := len(db.segments) - len(segments)
	db.segments = append(db.segments[:to], segment)
	db.mux.Unlock()

	for _, s := range segments {
		os.Remove(s.path)
	}
}
