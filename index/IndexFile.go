package index

import (
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
)

type IndexFile struct {
	file      *os.File
	size      int64
	memoryMap mmap.MMap
}

func OpenIndexFile(options Options) (*IndexFile, error) {
	fileMode := os.O_CREATE | os.O_RDWR
	file, err := os.OpenFile(options.FileName, fileMode, 0644)

	if err != nil {
		return nil, err
	}
	indexFile := &IndexFile{file: file}
	indexFile.size, _ = indexFile.fileSize()

	if indexFile.size > 0 {
		if err := indexFile.mMap(); err != nil {
			return nil, err
		}
	}
	return indexFile, nil
}

func (indexFile *IndexFile) ResizeTo(sizeInBytes int64) error {
	err := indexFile.unMap()
	if err != nil {
		return err
	}
	if err := indexFile.file.Truncate(sizeInBytes); err != nil {
		return err
	}

	indexFile.size = sizeInBytes
	return indexFile.mMap()
}

func (indexFile *IndexFile) Close() error {
	err := indexFile.unMap()
	if err != nil {
		return err
	}
	err = indexFile.file.Close()
	if err != nil {
		return err
	}
	return nil
}

func (indexFile *IndexFile) readFrom(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	elementsCopied := copy(buf, indexFile.memoryMap[offset:])
	if elementsCopied < size {
		return nil, io.EOF
	}
	return buf, nil
}

func (indexFile *IndexFile) writeAt(offset int64, buffer []byte) {
	copy(indexFile.memoryMap[offset:], buffer)
}

func (indexFile *IndexFile) fileSize() (int64, error) {
	stat, err := indexFile.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (indexFile *IndexFile) unMap() error {
	if indexFile.file == nil || indexFile.memoryMap == nil {
		return nil
	}
	return indexFile.memoryMap.Unmap()
}

func (indexFile *IndexFile) mMap() error {
	if err := indexFile.unMap(); err != nil {
		return err
	}
	memoryMapped, err := mmap.Map(indexFile.file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	indexFile.memoryMap = memoryMapped
	return nil
}
