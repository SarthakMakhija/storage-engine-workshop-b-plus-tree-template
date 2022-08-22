package index

import (
	"os"
	"testing"
)

func deleteFile(indexFile *IndexFile) {
	_ = os.Remove(indexFile.file.Name())
}

func TestCreatesANewIndexFileWithFileSize(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	defer deleteFile(indexFile)

	expectedFileSize := int64(0)
	actualFileSize := indexFile.size

	if actualFileSize != expectedFileSize {
		t.Fatalf("Expected file size to be %v, received %v", expectedFileSize, actualFileSize)
	}
}

func TestOpensAnExistingFileWithFileSizeGreaterThanZero(t *testing.T) {
	options := Options{
		PageSize: os.Getpagesize(),
		FileName: "./test",
	}
	createATestFileWithSize(options.FileName, options.PageSize)

	indexFile, _ := OpenIndexFile(options)
	defer deleteFile(indexFile)

	expectedFileSize := int64(options.PageSize)
	actualFileSize := indexFile.size

	if actualFileSize != expectedFileSize {
		t.Fatalf("Expected file size to be %v, received %v", expectedFileSize, actualFileSize)
	}
}

func TestResizesAnEmptyFileToAGivenSize(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	defer deleteFile(indexFile)

	_ = indexFile.ResizeTo(100)

	expectedFileSize := int64(100)
	actualFileSize := indexFile.size

	if actualFileSize != expectedFileSize {
		t.Fatalf("Expected file size to be %v, received %v", expectedFileSize, actualFileSize)
	}
}

func TestClosesTheIndexFile(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	defer deleteFile(indexFile)

	_ = indexFile.ResizeTo(100)

	err := indexFile.Close()
	if err != nil {
		t.Fatalf("Expected no error while closing the index file, but received %v", err)
	}
}

func createATestFileWithSize(fileName string, sizeBytes int) {
	file, _ := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	_, _ = file.Write(make([]byte, sizeBytes))
	_ = file.Close()
}
