package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var (
	pairs = [][]string {
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	newPairs = [][]string {
		{"key2", "value3"},
		{"key3", "value4"},
	}

	morePairs = [][]string {
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value6"},
		{"key7", "value7"},
		{"key8", "value8"},
		{"key9", "value9"},
		{"key10", "value10"},
		{"key11", "value11"},
		{"key12", "value12"},

	}
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}

	outFile, err := os.Open(filepath.Join(dir, segmentPrefix + activeSuffix))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()
	/*
		the current database has 10 MB active block size, so merge func won't be called
		and this "file growth" test will be deterministic
	 */

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 * 2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}
func TestDb_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDbSized(dir, 50)
	if err != nil {
		t.Fatal(err)
	}

	for _, pair := range pairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}

	files, err := ioutil.ReadDir(dir)
	if len(files) != 2 {
		t.Errorf("Unexpected segment count (%d vs %d)", len(files), 2)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDb_Merge(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDbSizedMerge(dir, 44, false)
	if err != nil {
		t.Fatal(err)
	}

	for _, pair := range pairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range newPairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}

	files, err := ioutil.ReadDir(dir)
	if len(files) != 3 {
		t.Errorf("Unexpected segment count before merge (%d vs %d)", len(files), 3)
	}

	db.merge()
	files, err = ioutil.ReadDir(dir)
	if len(files) != 2 {
		t.Errorf("Unexpected segment count after merge (%d vs %d)", len(files), 2)
	}

	mergedSegment := db.segments[1]
	expectedMergedSegment := [][]string {
		{"key1", "value1"},
		{"key2", "value3"},
		{"key3", "value3"},
	}

	for _, pair := range expectedMergedSegment {
		value, err := mergedSegment.get(pair[0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pair[0], err)
		}

		if value != pair[1] {
			t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDb_Concurrency(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDbSized(dir, 44)
	if err != nil {
		t.Fatal(err)
	}

	resCh := make(chan int)

	for _, pair := range morePairs {
		pair := pair
		go func() {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}

			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}

			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}

			resCh <- 1
		}()
	}

	for range morePairs {
		<- resCh
	}

	for _, pair := range morePairs {
		value, err := db.Get(pair[0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pair[0], err)
		}

		if value != pair[1] {
			t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
		}
	}

	// add this to check merging, but this should be checked manually, because time.Sleep isn't fully deterministic
	//time.Sleep(5 * time.Second)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDb_Delete(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDbSizedMerge(dir, 46, false)
	if err != nil {
		t.Fatal(err)
	}

	for _, pair := range morePairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}

	deleteKey := morePairs[5][0]

	err = db.Delete(deleteKey)
	if err != nil {
		t.Errorf("Cannot delete %s: %s", deleteKey, err)
	}

	value, err := db.Get(deleteKey)
	if err != ErrNotFound && value != "" {
		t.Errorf("Get value after it's being deleted %s: %s", deleteKey, err)
	}

	err = db.Delete(deleteKey)
	if err != nil {
		t.Errorf("Error occured while deleting item %s: %s", deleteKey, err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = NewDbSized(dir, 44)
	if err != nil {
		t.Fatal(err)
	}

	value, err = db.Get(deleteKey)
	if err != ErrNotFound && value != "" {
		t.Errorf("Get value after it's being deleted and database recreated %s: %s", deleteKey, err)
	}

	// after deletion active segment should have deleted marked record and return ErrItemDeleted error
	_, err = db.segments[0].get(deleteKey)
	if err != ErrItemDeleted {
		t.Errorf("Value not marked as deleted before merge %s: %s", deleteKey, err)
	}

	db.merge()

	// after merge record marked as deleted should be removed from segment and index table
	_, err = db.segments[1].get(deleteKey)
	if err != nil {
		t.Errorf("Value exists after merge %s: %s", deleteKey, err)
	}
}
