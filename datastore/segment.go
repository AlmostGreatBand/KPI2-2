package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const activeSuffix = "active"
const mergedSuffix = "merged"
const segmentPrefix = "segment-"
const bufSize = 8192
const deletedItemPos = -1

type segment struct {
	path   string
	offset int64
	index  hashIndex
}

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

			// we don't need to handle concurrency here, because recover is called before Db creation, and there is no
			// concurrent access to index(from put, get etc)
			if e.value == "" {
				s.index[e.key] = deletedItemPos
			} else {
				s.index[e.key] = s.offset
			}

			s.offset += int64(n)
		}
	}
	return err
}

func (s *segment) get(key string) (string, error) {

	position, ok := s.index[key]

	if !ok && position != 0 {
		print()
	}
	if !ok {
		return "", ErrNotFound
	}

	if position == deletedItemPos {
		return "", ErrItemDeleted
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
