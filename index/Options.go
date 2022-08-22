package index

import "os"

type Options struct {
	// PageSize for file I/O. All reads and writes will always
	// be done with pages of this size. Must be multiple of os.Getpagesize().
	PageSize int

	// Name of the index file
	FileName string

	// PreAllocatedPagePoolSize identifies the number of pages to be pre-allocated when the B+Tree is opened.
	// Must be greater than 0, it avoids mmap/unmap and truncate overhead during insertions
	PreAllocatedPagePoolSize int

	// AllowedPageOccupancyPercentage defines the amount of size that a page should occupy in bytes.
	// After this size, page will be split
	AllowedPageOccupancyPercentage int
}

func DefaultOptions() Options {
	return Options{
		PageSize:                       os.Getpagesize(),
		FileName:                       "index.db",
		PreAllocatedPagePoolSize:       10,
		AllowedPageOccupancyPercentage: 80,
	}
}
