package index

import (
	"os"
	"reflect"
	"testing"
)

func TestCreatesABPlusTreeByPreAllocatingPagesAlongWithMetaPageAndRootPage(t *testing.T) {
	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 6,
	}
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	expectedPageCount := options.PreAllocatedPagePoolSize + metaPageCount + rootPageCount
	actualPageCount := tree.pagePool.pageCount

	if actualPageCount != expectedPageCount {
		t.Fatalf("Expected %v page count, received %v page count", expectedPageCount, actualPageCount)
	}
}

func TestCreatesABPlusTreeWithFreePageList(t *testing.T) {
	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 6,
	}
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	expected := []int{2, 3, 4, 5, 6, 7} //first 2 pages for meta and root
	freePageIds := tree.freePageList.pageIds

	if !reflect.DeepEqual(expected, freePageIds) {
		t.Fatalf("Expected free pageIds to be %v, received %v", expected, freePageIds)
	}
}

func TestCreatesABPlusTreeWithARootPage(t *testing.T) {
	options := DefaultOptions()
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	if tree.pageHierarchy.rootPage == nil {
		t.Fatalf("Expected root page to be non-nil received nil")
	}
}

func TestCreatesABPlusTreeByCachingRootPage(t *testing.T) {
	options := DefaultOptions()
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	rootPageId := tree.pageHierarchy.RootPageId()
	rootPage := tree.pageHierarchy.PageById(rootPageId)

	if rootPage == nil {
		t.Fatalf("Expected root page in page cache to be non-nil received nil")
	}
}

func TestDoesNotGetByKeyAsSearchedKeyDoesNotExist(t *testing.T) {
	options := DefaultOptions()
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	tree.pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{key: []byte("A")},
		{key: []byte("B")},
	}

	getResult := tree.Get([]byte("C"))

	if getResult.found != false && getResult.Err != nil {
		t.Fatalf("Expected found to be false received %v, and error to be nil, received Err %v", getResult.found, getResult.Err)
	}
}

func TestGetsByKeyGivenKeyIsFoundInTheNonLeafPage(t *testing.T) {
	writeLeftPageToFile := func(fileName string, pageSize int) {
		leftPage := Page{
			id: 2,
			keyValuePairs: []KeyValuePair{
				{
					key:   []byte("A"),
					value: []byte("Storage"),
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
					value: []byte("Database"),
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
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	tree.pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{{key: []byte("B")}}

	writeLeftPageToFile(options.FileName, options.PageSize)
	writeRightPageToFile(options.FileName, options.PageSize)
	tree.pageHierarchy.rootPage.childPageIds = []int{2, 3}

	expectedKeyValuePair := KeyValuePair{
		key:   []byte("B"),
		value: []byte("Database"),
	}
	getResult := tree.Get([]byte("B"))

	if !expectedKeyValuePair.Equals(getResult.KeyValuePair) {
		t.Fatalf("Expected KeyValuePair to be %v, received %v", expectedKeyValuePair, getResult.KeyValuePair)
	}
}

func TestPutsAKeyValuePair(t *testing.T) {
	options := DefaultOptions()
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	tree.pageHierarchy.rootPage.keyValuePairs = []KeyValuePair{
		{
			key:   []byte("B"),
			value: []byte("Database"),
		},
	}
	_ = tree.Put([]byte("C"), []byte("Storage"))
	expected := []KeyValuePair{
		{key: []byte("B"), value: []byte("Database")},
		{key: []byte("C"), value: []byte("Storage")},
	}

	pageKeyValuePairs := tree.pageHierarchy.rootPage.AllKeyValuePairs()
	if !reflect.DeepEqual(expected, pageKeyValuePairs) {
		t.Fatalf("Expected Key value pairs to be %v, received %v", expected, pageKeyValuePairs)
	}
}

func TestClosesBPlusTree(t *testing.T) {
	options := Options{
		PageSize:                 os.Getpagesize(),
		FileName:                 "./test",
		PreAllocatedPagePoolSize: 6,
	}
	tree, _ := CreateBPlusTree(options)
	defer deleteFile(tree.pagePool.indexFile)

	err := tree.Close()
	if err != nil {
		t.Fatalf("Expected no error while closing the BPlusTree file, but received %v", err)
	}
}
