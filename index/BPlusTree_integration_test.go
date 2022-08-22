package index

import (
	"os"
	"strconv"
	"testing"
)

func TestPutsAndGets1000KeyValuePairsWithDefaultOptions(t *testing.T) {
	options := DefaultOptions()
	bPlusTree, _ := CreateBPlusTree(options)
	defer deleteFile(bPlusTree.pagePool.indexFile)

	for index := 1; index <= 1000; index++ {
		err := bPlusTree.Put(
			[]byte("Key"+strconv.Itoa(index)),
			[]byte("Value"+strconv.Itoa(index)),
		)
		if err != nil {
			t.Fatalf("Failed while inserting %v", err)
		}
	}
	for index := 1; index <= 1000; index++ {
		key := []byte("Key" + strconv.Itoa(index))
		getResult := bPlusTree.Get(
			key,
		)
		if getResult.KeyValuePair.isEmpty() {
			t.Fatalf("Received empty key value pair for %v", string(key))
		}
		expected := KeyValuePair{
			key:   key,
			value: []byte("Value" + strconv.Itoa(index)),
		}
		if !expected.Equals(getResult.KeyValuePair) {
			t.Fatalf("Expected key value pair to be %v, received %v", expected, getResult.KeyValuePair)
		}
	}
}

func TestPutsAndGets10000KeyValuePairsWithDefaultOptions(t *testing.T) {
	options := DefaultOptions()
	bPlusTree, _ := CreateBPlusTree(options)
	defer deleteFile(bPlusTree.pagePool.indexFile)

	for index := 1; index <= 10000; index++ {
		err := bPlusTree.Put(
			[]byte("Key"+strconv.Itoa(index)),
			[]byte("Value"+strconv.Itoa(index)),
		)
		if err != nil {
			t.Fatalf("Failed while inserting %v", err)
		}
	}
	for index := 1; index <= 10000; index++ {
		key := []byte("Key" + strconv.Itoa(index))
		getResult := bPlusTree.Get(
			key,
		)
		if getResult.KeyValuePair.isEmpty() {
			t.Fatalf("Received empty key value pair for %v", string(key))
		}
		expected := KeyValuePair{
			key:   key,
			value: []byte("Value" + strconv.Itoa(index)),
		}
		if !expected.Equals(getResult.KeyValuePair) {
			t.Fatalf("Expected key value pair to be %v, received %v", expected, getResult.KeyValuePair)
		}
	}
}

func TestPutsAndGets10000KeyValuePairsWithCustomOptionsToForceSplits(t *testing.T) {
	options := Options{
		FileName:                       "./index.db",
		PageSize:                       os.Getpagesize(),
		AllowedPageOccupancyPercentage: 20,
		PreAllocatedPagePoolSize:       10,
	}
	bPlusTree, _ := CreateBPlusTree(options)
	defer deleteFile(bPlusTree.pagePool.indexFile)

	for index := 1; index <= 10000; index++ {
		err := bPlusTree.Put(
			[]byte("Key"+strconv.Itoa(index)),
			[]byte("Value"+strconv.Itoa(index)),
		)
		if err != nil {
			t.Fatalf("Failed while inserting %v", err)
		}
	}
	for index := 1; index <= 10000; index++ {
		key := []byte("Key" + strconv.Itoa(index))
		getResult := bPlusTree.Get(
			key,
		)
		if getResult.KeyValuePair.isEmpty() {
			t.Fatalf("Received empty key value pair for %v", string(key))
		}
		expected := KeyValuePair{
			key:   key,
			value: []byte("Value" + strconv.Itoa(index)),
		}
		if !expected.Equals(getResult.KeyValuePair) {
			t.Fatalf("Expected key value pair to be %v, received %v", expected, getResult.KeyValuePair)
		}
	}
}
