package index

import (
	"os"
	"reflect"
	"testing"
)

func TestReturnsThePageCountInAnIndexFile(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	expectedPageCount := 0
	actualPageCount := pagePool.pageCount

	if actualPageCount != expectedPageCount {
		t.Fatalf("Expected page count to be %v, received %v", expectedPageCount, actualPageCount)
	}
}

func TestReturnsTrueGivenIndexFileContainsZeroPages(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	containsZeroPages := pagePool.ContainsZeroPages()

	if containsZeroPages != true {
		t.Fatalf("Expected zero pages to be true")
	}
}

func TestReturnsFalseGivenIndexFileContainsMoreThanZeroPages(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	writeToATestFileWithEmptyPage(options.FileName, options.PageSize)

	indexFile, _ := OpenIndexFile(options)
	defer deleteFile(indexFile)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	containsZeroPages := pagePool.ContainsZeroPages()

	if containsZeroPages != false {
		t.Fatalf("Expected zero pages to be false")
	}
}

func TestAllocates5Pages(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	_, _ = pagePool.Allocate(5)
	expectedPageCount := 5
	actualPageCount := pagePool.pageCount

	if actualPageCount != expectedPageCount {
		t.Fatalf("Expected page count to be %v, received %v", expectedPageCount, actualPageCount)
	}
}

func TestReturnsTheCurrentPageIdAndAllocates5Pages(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	pageId, _ := pagePool.Allocate(5)
	expectedPageId := 0

	if pageId != expectedPageId {
		t.Fatalf("Expected page id to be %v, received %v", expectedPageId, pageId)
	}
}

func TestReturnsTheNextPageIdAfterAllocating5Pages(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	_, _ = pagePool.Allocate(5)
	expectedPageId := 5
	pageId := pagePool.pageCount

	if pageId != expectedPageId {
		t.Fatalf("Expected page id to be %v, received %v", expectedPageId, pageId)
	}
}

func TestAllocationOf5PagesShouldIncreaseTheFileSize(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	defer deleteFile(indexFile)

	_, _ = pagePool.Allocate(5)
	expectedFileSize := int64(5 * os.Getpagesize())
	actualFileSize := pagePool.indexFile.size

	if actualFileSize != expectedFileSize {
		t.Fatalf("Expected file size to be %v, received %v", expectedFileSize, actualFileSize)
	}
}

func TestReadsAPageIdentifiedByPageId0(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("A"),
				value: []byte("Storage"),
			},
		},
	}

	writeToATestFileWithEmptyPage(options.FileName, options.PageSize)
	writeToAATestFileWith(options.FileName, page.MarshalBinary())

	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	defer deleteFile(indexFile)

	pageId := 0
	readPage, _ := pagePool.Read(pageId)
	expectedKeyValuePair := page.keyValuePairs[0]

	if !expectedKeyValuePair.Equals(readPage.keyValuePairs[0]) {
		t.Fatalf("Expected key value pair to be %v, received %v", expectedKeyValuePair, readPage.keyValuePairs[0])
	}
}

func TestReadsAPageIdentifiedByPageId1(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("B"),
				value: []byte("Database Storage"),
			},
		},
	}
	pageOffset := int64(options.PageSize)
	writeToATestFileWithEmptyPage(options.FileName, options.PageSize*2)
	writeToAATestFileAtOffset(options.FileName, page.MarshalBinary(), pageOffset)

	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	defer deleteFile(indexFile)

	pageId := 1
	readPage, _ := pagePool.Read(pageId)
	expectedKeyValuePair := page.keyValuePairs[0]

	if !expectedKeyValuePair.Equals(readPage.keyValuePairs[0]) {
		t.Fatalf("Expected key value pair to be %v, received %v", expectedKeyValuePair, readPage.keyValuePairs[0])
	}
}

func TestWritesAPage(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	page := &Page{
		id: 0,
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("A"),
				value: []byte("Storage"),
			},
		},
	}

	writeToATestFileWithEmptyPage(options.FileName, options.PageSize)

	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	defer deleteFile(indexFile)

	pagePool.Write(page)

	readPage, _ := pagePool.Read(page.id)
	expectedKeyValuePair := page.keyValuePairs[0]

	if !expectedKeyValuePair.Equals(readPage.keyValuePairs[0]) {
		t.Fatalf("Expected key value pair to be %v, received %v", expectedKeyValuePair, readPage.keyValuePairs[0])
	}
}

func TestReadsANonLeafPageWithChildPageIds(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
		},
		childPageIds: []int{10, 20},
	}

	writeToATestFileWithEmptyPage(options.FileName, options.PageSize)
	writeToAATestFileWith(options.FileName, page.MarshalBinary())

	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	defer deleteFile(indexFile)

	pageId := 0
	readPage, _ := pagePool.Read(pageId)

	expected := []int{10, 20}
	childPageIds := readPage.childPageIds

	if !reflect.DeepEqual(expected, childPageIds) {
		t.Fatalf("Expected child page ids to be %v, received %v", expected, childPageIds)
	}
}

func TestReadsANonLeafPageWithKeyValuePairs(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
		},
		childPageIds: []int{10, 20},
	}

	writeToATestFileWithEmptyPage(options.FileName, options.PageSize)
	writeToAATestFileWith(options.FileName, page.MarshalBinary())

	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	defer deleteFile(indexFile)

	pageId := 0
	readPage, _ := pagePool.Read(pageId)

	expected := []KeyValuePair{{key: []byte("A")}}
	keyValuePairs := readPage.AllKeyValuePairs()

	if !reflect.DeepEqual(expected, keyValuePairs) {
		t.Fatalf("Expected keyValuePairs to be %v, received %v", expected, keyValuePairs)
	}
}

func writeToATestFileWithEmptyPage(fileName string, pageSize int) {
	writeToAATestFileWith(fileName, make([]byte, pageSize))
}

func writeToAATestFileWith(fileName string, content []byte) {
	writeToAATestFileAtOffset(fileName, content, 0)
}

func writeToAATestFileAtOffset(fileName string, content []byte, offset int64) {
	file, _ := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	_, _ = file.Seek(offset, 0)
	_, _ = file.Write(content)
	_ = file.Close()
}
