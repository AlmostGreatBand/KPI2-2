package datastore

import (
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


const defMaxActiveSize = 10 * 1024 * 1024

var ErrNotFound = fmt.Errorf("record does not exist")
var ErrItemDeleted = fmt.Errorf("record has been deleted")

type hashIndex map[string]int64

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
			db.put(el)
		}
	}()

	return db, nil
}

func (db *Db) Close() error {
	db.mergeChan <- 0
	close(db.putChan)
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
		value, err = segment.get(key)
		if err == ErrItemDeleted {
			return "", ErrNotFound
		}
		if err == nil {
			return value, nil
		}
	}

	return "", err
}

func (db *Db) Put(key, value string) error {
	responseChan := make(chan error)
	e := &entry{ key: key, value: value }

	db.putChan <- putEntry{ entry: e, responseChan: responseChan }
	res := <- responseChan
	return res
}

func (db *Db) put(pe putEntry) {
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

	// we need this lock to be sure that we won't have concurrent read/write from get/put in our map(index)
	// we could create mutex in every segment and lock it, but have a mutex instance in every segment
	// is overkill in our opinion, so we decided to lock entire database
	db.mux.Lock()
	activeSegment := db.segments[0]
	if e.value != "" {
		activeSegment.index[e.key] = activeSegment.offset
	} else {
		activeSegment.index[e.key] = deletedItemPos
	}
	activeSegment.offset += int64(n)
	db.mux.Unlock()

	fi, err := os.Stat(activeSegment.path)
	if err != nil {
		fmt.Errorf("can not read active file stat: %v", err)
		// return nil because we have already put value to db and user shouldn't know about segmentation error
		pe.responseChan <- nil
		return
	}

	if fi.Size() >= db.activeBlockSize {
		_, err = db.addSegment()
		if err != nil {
			// return nil because we have already put value to db and user shouldn't know about segmentation error
			pe.responseChan <- nil
			return
		}
	}

	pe.responseChan <- nil
}

func (db *Db) Delete(key string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	for _, segment := range db.segments {
		pos, ok := segment.index[key]
		if pos == deletedItemPos {
			break
		}

		if ok {
			e := &entry{key: key}
			n, err := db.out.Write(e.EncodeDeleted())
			if err != nil {
				return err
			}

			activeSegment := db.segments[0]
			activeSegment.index[key] = deletedItemPos
			activeSegment.offset += int64(n)
			break
		}
	}

	return nil
}

func (db *Db) addSegment() (*segment, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	err := db.out.Close()
	if err != nil {
		return nil, err
	}

	segmentSuffix := 0
	if len(db.segments) > 1 {
		lastSavedSegmentSuffix := db.segments[1].path[len(db.dir + segmentPrefix) + 1:]
		if prevSegmentSuffix, err := strconv.Atoi(lastSavedSegmentSuffix); err == nil {
			segmentSuffix = prevSegmentSuffix + 1
		}
	}

	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%v%v", segmentPrefix, segmentSuffix))
	outputPath := filepath.Join(db.dir, segmentPrefix + activeSuffix)

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
		for k, p := range segments[i].index {
			if p != deletedItemPos {
				keysSegments[k] = s
			}
		}
	}

	segmentPath := filepath.Join(db.dir, segmentPrefix)
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
		if value != "" && err == nil {
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
	}

	db.mux.Lock()

	mergedPath := segmentPath + mergedSuffix
	err = os.Rename(segmentPath, mergedPath)
	if err != nil {
		db.mux.Unlock()
		fmt.Errorf("cannot merge files: %v", err)
		return
	}
	segment.path = mergedPath
	to := len(db.segments) - len(segments)
	db.segments = append(db.segments[:to], segment)

	db.mux.Unlock()

	for _, s := range segments {
		if mergedPath != s.path {
			os.Remove(s.path)
		}
	}
}
