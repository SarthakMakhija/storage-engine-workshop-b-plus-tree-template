package index

import (
	"os"
	"reflect"
	"testing"
)

func DefaultFreePageList(pageCount int) *FreePageList {
	return DefaultFreePageListWithStartingPgeId(2, pageCount)
}

func DefaultFreePageListWithStartingPgeId(startingPageId, pageCount int) *FreePageList {
	return InitializeFreePageList(startingPageId, pageCount)
}

func TestReturnsPageById(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	pageHierarchy.pageById[0] = &Page{
		id: 0,
	}

	defer deleteFile(pagePool.indexFile)

	page := pageHierarchy.PageById(0)
	if page.id != 0 {
		t.Fatalf("Expected page id to be 0 received %v", page.id)
	}
}

func TestReturnsTheRootPageId(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	pageHierarchy.rootPage = &Page{id: 100}

	defer deleteFile(pagePool.indexFile)

	rootPageId := pageHierarchy.RootPageId()
	if rootPageId != 100 {
		t.Fatalf("Expected root page id to be 100 received %v", rootPageId)
	}
}

func TestReturnsTrueGivenPageIsEligibleForSplit(t *testing.T) {
	options := Options{
		PageSize:                       100,
		AllowedPageOccupancyPercentage: 1,
		FileName:                       "./test",
		PreAllocatedPagePoolSize:       8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 2, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	page := &Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
			{key: []byte("B")},
		},
	}

	defer deleteFile(pagePool.indexFile)

	isEligibleForSplit := pageHierarchy.isPageEligibleForSplit(page)
	if isEligibleForSplit != true {
		t.Fatalf("Expected page to be eligible for split but received false")
	}
}

func TestReturnsFalseGivenPageIsNotEligibleForSplit(t *testing.T) {
	options := Options{
		PageSize:                       4096,
		AllowedPageOccupancyPercentage: 90,
		FileName:                       "./test",
		PreAllocatedPagePoolSize:       8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	page := &Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
			{key: []byte("B")},
		},
	}

	defer deleteFile(pagePool.indexFile)

	isEligibleForSplit := pageHierarchy.isPageEligibleForSplit(page)
	if isEligibleForSplit != false {
		t.Fatalf("Expected page to be non eligible for split but received true")
	}
}

func TestDoesNotGetByKey(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{key: []byte("A")},
		{key: []byte("B")},
	}

	getResult := pageHierarchy.Get([]byte("C"))

	if getResult.found != false && getResult.Err != nil {
		t.Fatalf("Expected found to be false received %v, and error to be nil, received Err %v", getResult.found, getResult.Err)
	}
}

func TestGetsByKeyInRootLeafPage(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("B"),
			value: []byte("Systems"),
		},
	}

	expectedKeyValuePair := KeyValuePair{
		key:   []byte("B"),
		value: []byte("Systems"),
	}
	getResult := pageHierarchy.Get([]byte("B"))

	if !expectedKeyValuePair.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected KeyValuePair to be %v, received %v", expectedKeyValuePair, getResult.KeyValuePair)
	}
}

func TestGetsByKeyInTheLeafPageWhichIsTheLeftChildOfRootPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) {
		leftPage := Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
	}
	writeRightPageToFile := func(fileName string, pageSize int) {
		rightPage := Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
	}

	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	writeLeftPageToFile(options.FileName, options.PageSize)
	writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}

	expectedKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Database"),
	}
	getResult := pageHierarchy.Get([]byte("A"))

	if !expectedKeyValuePair.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected KeyValuePair to be %v, received %v", expectedKeyValuePair, getResult.KeyValuePair)
	}
}

func TestGetsByKeyInTheLeafPageWhichIsTheRightChildOfRootPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) {
		leftPage := Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
	}
	writeRightPageToFile := func(fileName string, pageSize int) {
		rightPage := Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
	}

	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	writeLeftPageToFile(options.FileName, options.PageSize)
	writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}

	expectedKeyValuePair := KeyValuePair{
		key:   []byte("C"),
		value: []byte("Systems"),
	}
	getResult := pageHierarchy.Get([]byte("C"))

	if !expectedKeyValuePair.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected KeyValuePair to be %v, received %v", expectedKeyValuePair, getResult.KeyValuePair)
	}
}

func TestGetsByKeyInTheLeafPageWhichIsTheRightChildOfRootPageGivenKeyIsFoundInTheNonLeafPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) {
		leftPage := Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
	}
	writeRightPageToFile := func(fileName string, pageSize int) {
		rightPage := Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
	}

	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	writeLeftPageToFile(options.FileName, options.PageSize)
	writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}

	expectedKeyValuePair := KeyValuePair{
		key:   []byte("B"),
		value: []byte("Storage"),
	}
	getResult := pageHierarchy.Get([]byte("B"))

	if !expectedKeyValuePair.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected KeyValuePair to be %v, received %v", expectedKeyValuePair, getResult.KeyValuePair)
	}
}

func TestPutsAKeyValuePairInRootLeafPage(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
	}
	_ = pageHierarchy.Put(KeyValuePair{key: []byte("B"), value: []byte("Storage")})

	expected := []KeyValuePair{
		{key: []byte("A"), value: []byte("Database")},
		{key: []byte("B"), value: []byte("Storage")},
		{key: []byte("C"), value: []byte("Systems")},
	}

	pageKeyValuePairs := pageHierarchy.rootPage.AllKeyValuePairs()
	if !reflect.DeepEqual(expected, pageKeyValuePairs) {
		t.Fatalf("Expected Key value pairs to be %v, received %v", expected, pageKeyValuePairs)
	}
}

func TestUpdatesAKeyValuePairInRootLeafPage(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
	}
	_ = pageHierarchy.Put(KeyValuePair{key: []byte("C"), value: []byte("OS")})

	expected := []KeyValuePair{
		{key: []byte("A"), value: []byte("Database")},
		{key: []byte("C"), value: []byte("OS")},
	}

	pageKeyValuePairs := pageHierarchy.rootPage.AllKeyValuePairs()
	if !reflect.DeepEqual(expected, pageKeyValuePairs) {
		t.Fatalf("Expected Key value pairs to be %v, received %v", expected, pageKeyValuePairs)
	}
}

func TestPutsAKeyValuePairInTheRightPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) {
		leftPage := Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
	}
	writeRightPageToFile := func(fileName string, pageSize int) {
		rightPage := Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
	}

	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	writeLeftPageToFile(options.FileName, options.PageSize)
	writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("D"), value: []byte("OS")})

	getResult := pageHierarchy.Get([]byte("D"))
	expected := KeyValuePair{key: []byte("D"), value: []byte("OS")}

	if !expected.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected Key value pair to be %v, received %v", expected, getResult.KeyValuePair)
	}
}

func TestPutsAKeyValuePairAfterSplittingTheRootPage(t *testing.T) {
	options := DefaultOptions()
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)
	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("E"),
			value: []byte("OS"),
		},
	}

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("D"), value: []byte("File System")})

	getResult := pageHierarchy.Get([]byte("D"))
	expected := KeyValuePair{key: []byte("D"), value: []byte("File System")}

	if !expected.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected Key value pair to be %v, received %v", expected, getResult.KeyValuePair)
	}
}

func TestSplitsTheRootPageAndCreatesANewRootWithKeyValuePairs(t *testing.T) {
	options := Options{
		PageSize:                 200,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)
	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("E"),
			value: []byte("OS"),
		},
	}

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("D"), value: []byte("File System")})

	keyValuePairsOfNewRootPage := pageHierarchy.rootPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("C")}}

	if !reflect.DeepEqual(expected, keyValuePairsOfNewRootPage) {
		t.Fatalf("Expected Key value pair in the new root to be %v, received %v", expected, keyValuePairsOfNewRootPage)
	}
}

func TestSplitsTheRootPageAndWithKeyValuePairsInOldRoot(t *testing.T) {
	options := Options{
		PageSize:                 100,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)
	existingRootPage := pageHierarchy.rootPage
	existingRootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("E"),
			value: []byte("OS"),
		},
	}

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("D"), value: []byte("File System")})

	keyValuePairs := existingRootPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("A"), value: []byte("Database")}}

	if !reflect.DeepEqual(expected, keyValuePairs) {
		t.Fatalf("Expected Key value pair in the old root to be %v, received %v", expected, keyValuePairs)
	}
}

func TestSplitsTheRootPageAndWithKeyValuePairsInRightSiblingPage(t *testing.T) {
	options := Options{
		PageSize:                 200,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)
	existingRootPage := pageHierarchy.rootPage
	existingRootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("A"),
			value: []byte("Database"),
		},
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("E"),
			value: []byte("OS"),
		},
	}

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("D"), value: []byte("File System")})
	rightSibling, _ := pagePool.Read(pageHierarchy.rootPage.childPageIds[1])
	keyValuePairs := rightSibling.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("C"), value: []byte("Systems")}, {key: []byte("D"), value: []byte("File System")}, {key: []byte("E"), value: []byte("OS")}}

	if !reflect.DeepEqual(expected, keyValuePairs) {
		t.Fatalf("Expected Key value pair in the right sibling to be %v, received %v", expected, keyValuePairs)
	}
}

func TestSplitsLeafPageAndAddsAKeyToTheRootPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) *Page {
		leftPage := &Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
		return leftPage
	}
	writeRightPageToFile := func(fileName string, pageSize int) *Page {
		rightPage := &Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
				{
					key:   []byte("D"),
					value: []byte("OS"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
		return rightPage
	}

	options := Options{
		PageSize:                 200,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageListWithStartingPgeId(4, options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	leftPage := writeLeftPageToFile(options.FileName, options.PageSize)
	rightPage := writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}
	pageHierarchy.pageById[2] = leftPage
	pageHierarchy.pageById[3] = rightPage

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("E"), value: []byte("NFS")})

	expected := []KeyValuePair{{key: []byte("B")}, {key: []byte("C")}}
	rootPageKeyValuePairs := pageHierarchy.rootPage.AllKeyValuePairs()

	if !reflect.DeepEqual(expected, rootPageKeyValuePairs) {
		t.Fatalf("Expected Key value pair in the root page to be %v, received %v", expected, rootPageKeyValuePairs)
	}
}

func TestSplitsLeafPageAndPutsTheValueInTheRightSibling(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) *Page {
		leftPage := &Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
		return leftPage
	}
	writeRightPageToFile := func(fileName string, pageSize int) *Page {
		rightPage := &Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
				{
					key:   []byte("D"),
					value: []byte("OS"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
		return rightPage
	}

	options := Options{
		PageSize:                 200,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageListWithStartingPgeId(4, options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	leftPage := writeLeftPageToFile(options.FileName, options.PageSize)
	rightPage := writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}
	pageHierarchy.pageById[2] = leftPage
	pageHierarchy.pageById[3] = rightPage

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("E"), value: []byte("NFS")})
	getResult := pageHierarchy.Get([]byte("E"))
	resultantPage := getResult.page

	expected := []KeyValuePair{
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("D"),
			value: []byte("OS"),
		},
		{
			key:   []byte("E"),
			value: []byte("NFS"),
		},
	}
	resultantPageKeyValuePairs := resultantPage.AllKeyValuePairs()

	if !reflect.DeepEqual(expected, resultantPageKeyValuePairs) {
		t.Fatalf("Expected Key value pair in the sibling page to be %v, received %v", expected, resultantPageKeyValuePairs)
	}
}

func TestSplitsLeafPageAndAddsTheNewPageAsTheRightmostChildOfTheRootPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) *Page {
		leftPage := &Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, leftPage.MarshalBinary(), int64(pageSize*leftPage.id))
		return leftPage
	}
	writeRightPageToFile := func(fileName string, pageSize int) *Page {
		rightPage := &Page{
			id: 3,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("B"),
					value: []byte("Storage"),
				},
				{
					key:   []byte("C"),
					value: []byte("Systems"),
				},
				{
					key:   []byte("D"),
					value: []byte("OS"),
				},
			},
		}
		writeToAATestFileAtOffset(fileName, rightPage.MarshalBinary(), int64(pageSize*rightPage.id))
		return rightPage
	}

	options := Options{
		PageSize:                 200,
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageListWithStartingPgeId(4, options.PreAllocatedPagePoolSize))

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	leftPage := writeLeftPageToFile(options.FileName, options.PageSize)
	rightPage := writeRightPageToFile(options.FileName, options.PageSize)
	pageHierarchy.rootPage.childPageIds = []int{2, 3}
	pageHierarchy.pageById[2] = leftPage
	pageHierarchy.pageById[3] = rightPage

	_ = pageHierarchy.Put(KeyValuePair{key: []byte("E"), value: []byte("NFS")})
	resultantPageId := pageHierarchy.rootPage.childPageIds[len(pageHierarchy.rootPage.childPageIds)-1]
	resultantPage, _ := pagePool.Read(resultantPageId)

	expected := []KeyValuePair{
		{
			key:   []byte("C"),
			value: []byte("Systems"),
		},
		{
			key:   []byte("D"),
			value: []byte("OS"),
		},
		{
			key:   []byte("E"),
			value: []byte("NFS"),
		},
	}
	resultantPageKeyValuePairs := resultantPage.AllKeyValuePairs()

	if !reflect.DeepEqual(expected, resultantPageKeyValuePairs) {
		t.Fatalf("Expected Key value pair in the sibling page to be %v, received %v", expected, resultantPageKeyValuePairs)
	}
}

func TestAllocatesPagesFromPagePoolGivenFreePageListIsEmpty(t *testing.T) {
	options := Options{
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 5,
		PageSize:                 100,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	emptyFreePageList := &FreePageList{}
	pageHierarchy := NewPageHierarchy(pagePool, 10, emptyFreePageList)

	defer deleteFile(pagePool.indexFile)

	pages, _ := pageHierarchy.allocatePages(2)
	expectedPageIds := []int{5, 6}

	if pages[0].id != expectedPageIds[0] {
		t.Fatalf("Expected first page id to be %v, received %v", expectedPageIds[0], pages[0].id)
	}
	if pages[1].id != expectedPageIds[1] {
		t.Fatalf("Expected second page id to be %v, received %v", expectedPageIds[1], pages[1].id)
	}
}

func TestWritesDirtyPagesToStorage(t *testing.T) {
	pageA := func() *Page {
		return &Page{
			id: 0,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Database"),
				},
			},
		}
	}

	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 8,
	}
	indexFile, _ := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)
	_, _ = pagePool.Allocate(options.PreAllocatedPagePoolSize)
	pageHierarchy := NewPageHierarchy(pagePool, 10, DefaultFreePageList(options.PreAllocatedPagePoolSize))

	pageHierarchy.pageById[0] = pageA()

	defer deleteFile(pagePool.indexFile)

	pageHierarchy.Write([]DirtyPage{{page: pageA()}})

	readPage, _ := pagePool.Read(pageA().id)
	expectedKeyValuePair := pageA().keyValuePairs[0]

	if !expectedKeyValuePair.Equals(readPage.keyValuePairs[0]) {
		t.Fatalf("Expected key value pair to be %v, received %v", expectedKeyValuePair, readPage.keyValuePairs[0])
	}
}
