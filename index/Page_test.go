package index

import (
	"reflect"
	"testing"
)

func TestGetsTheIndexForAKey(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
			{key: []byte("B")},
			{key: []byte("C")},
		},
	}
	expectedIndex := 0
	index, _ := page.Get([]byte("A"))

	if index != expectedIndex {
		t.Fatalf("Expected index of searched key A to be %v, received %v", expectedIndex, index)
	}
}

func TestReturnsTrueIfKeyIsPresentInThePage(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
			{key: []byte("B")},
			{key: []byte("C")},
		},
	}
	_, found := page.Get([]byte("B"))

	if found != true {
		t.Fatalf("Expected A to be found")
	}
}

func TestReturnsFalseIfKeyIsNotPresentInThePage(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("C")},
		},
	}
	_, found := page.Get([]byte("D"))

	if found != false {
		t.Fatalf("Expected A to not be found")
	}
}

func TestUnMarshalsAPageWithKeyValuePairCountAs1(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("C"),
				value: []byte("Storage"),
			},
		},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	keyValuePairCount := len(newPage.keyValuePairs)
	if keyValuePairCount != 1 {
		t.Fatalf("Expected keyValuePairCount to be 1, received %v", keyValuePairCount)
	}
}

func TestUnMarshalsAPageWithKey(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("C"),
				value: []byte("Storage"),
			},
		},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	key := string(newPage.keyValuePairs[0].key)
	if key != "C" {
		t.Fatalf("Expected key to be C, received %v", key)
	}
}

func TestUnMarshalsAPageWithValue(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("C"),
				value: []byte("Storage"),
			},
		},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	value := newPage.keyValuePairs[0].PrettyValue()
	if value != "Storage" {
		t.Fatalf("Expected value to be Storage, received %v", value)
	}
}

func TestUnMarshalsANonLeafPageWithKey(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key: []byte("C"),
			},
		},
		childPageIds: []int{10, 0},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	key := string(newPage.AllKeyValuePairs()[0].key)
	if key != "C" {
		t.Fatalf("Expected key to be C, received %v", key)
	}
}

func TestUnMarshalsANonLeafPageWithChildPageId(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key: []byte("C"),
			},
		},
		childPageIds: []int{10, 0},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	childPageId0 := newPage.childPageIds[0]
	if childPageId0 != 10 {
		t.Fatalf("Expected zeroth child page id to be 10, received %v", childPageId0)
	}
}

func TestUnMarshalsANonLeafPageWithMultipleChildPageIds(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("C")},
			{key: []byte("D")},
		},
		childPageIds: []int{10, 15, 20},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	expected := []int{10, 15, 20}
	childPageIds := newPage.childPageIds

	if !reflect.DeepEqual(childPageIds, expected) {
		t.Fatalf("Expected child page ids to be %v, received %v", expected, childPageIds)
	}
}

func TestUnMarshalsAPageWithMultipleKeyValuePairs(t *testing.T) {
	page := Page{
		keyValuePairs: []KeyValuePair{
			{
				key:   []byte("A"),
				value: []byte("Database"),
			},
			{
				key:   []byte("B"),
				value: []byte("Storage"),
			},
		},
	}
	bytes := page.MarshalBinary()

	newPage := &Page{}
	newPage.UnMarshalBinary(bytes)

	keyValuePairCount := len(newPage.keyValuePairs)
	if keyValuePairCount != 2 {
		t.Fatalf("Expected keyValuePairCount to be 2, received %v", keyValuePairCount)
	}

	expectedFirstKeyValuePair := page.keyValuePairs[0]
	firstKeyValuePair := newPage.keyValuePairs[0]

	if !expectedFirstKeyValuePair.Equals(firstKeyValuePair) {
		t.Fatalf("Expected first key value pair to be %v, received %v", expectedFirstKeyValuePair, firstKeyValuePair)
	}

	expectedSecondKeyValuePair := page.keyValuePairs[1]
	secondKeyValuePair := newPage.keyValuePairs[1]

	if !expectedSecondKeyValuePair.Equals(secondKeyValuePair) {
		t.Fatalf("Expected second key value pair to be %v, received %v", expectedSecondKeyValuePair, secondKeyValuePair)
	}
}

func TestInsertsAtAnIndexInAPageWhichIsLeaf(t *testing.T) {
	page := &Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A"), value: []byte("Database")},
			{key: []byte("C"), value: []byte("Storage")},
			{key: []byte("F"), value: []byte("Systems")},
		},
	}
	page.insertAt(2, KeyValuePair{key: []byte("D"), value: []byte("Operating")})
	expected := []KeyValuePair{
		{key: []byte("A"), value: []byte("Database")},
		{key: []byte("C"), value: []byte("Storage")},
		{key: []byte("D"), value: []byte("Operating")},
		{key: []byte("F"), value: []byte("Systems")},
	}

	pageKeyValuePairs := page.keyValuePairs
	if !reflect.DeepEqual(expected, pageKeyValuePairs) {
		t.Fatalf("Expected Key value pairs to be %v, received %v", expected, pageKeyValuePairs)
	}
}

func TestInsertsAtAnIndexInAPageWhichIsNonLeaf(t *testing.T) {
	page := &Page{
		keyValuePairs: []KeyValuePair{
			{key: []byte("A")},
		},
		childPageIds: []int{1},
	}
	page.insertAt(1, KeyValuePair{key: []byte("D"), value: []byte("Operating")})
	expected := []KeyValuePair{
		{key: []byte("A")},
		{key: []byte("D")},
	}

	pageKeyValuePairs := page.keyValuePairs
	if !reflect.DeepEqual(expected, pageKeyValuePairs) {
		t.Fatalf("Expected Key value pairs to be %v, received %v", expected, pageKeyValuePairs)
	}
}

func TestInsertsChildPageAtAnIndex(t *testing.T) {
	page := &Page{
		childPageIds: []int{8, 10, 14},
	}
	childPage := NewPage(11)
	expected := []int{8, 10, 11, 14}
	page.insertChildAt(2, childPage)

	actualChildPageId := page.childPageIds
	if !reflect.DeepEqual(expected, actualChildPageId) {
		t.Fatalf("Expected child page ids to be %v, received %v", expected, actualChildPageId)
	}
}

func TestSplitsALeafPageWithKeyValuePairs(t *testing.T) {
	page := &Page{
		id:            0,
		keyValuePairs: []KeyValuePair{{key: []byte("A"), value: []byte("Database")}, {key: []byte("B"), value: []byte("Systems")}},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{0}
	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 0)

	keyValuePairsAfterSplit := page.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("A"), value: []byte("Database")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsALeafPageWithKeyValuePairsInParent(t *testing.T) {
	page := &Page{
		id:            0,
		keyValuePairs: []KeyValuePair{{key: []byte("A"), value: []byte("Database")}, {key: []byte("B"), value: []byte("Systems")}},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{0}
	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 0)

	keyValuePairsAfterSplit := parentPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("B")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the parent page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsALeafPageWithKeyValuePairsInSibling(t *testing.T) {
	page := &Page{
		id:            0,
		keyValuePairs: []KeyValuePair{{key: []byte("A"), value: []byte("Database")}, {key: []byte("B"), value: []byte("Systems")}},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{0}
	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 0)

	keyValuePairsAfterSplit := siblingPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("B"), value: []byte("Systems")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the parent page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithKeyValuePairsWithEvenNumberOfKeyValuePairs(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13, 14},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	keyValuePairsAfterSplit := page.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("Q")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithKeyValuePairsWithOddNumberOfKeyValuePairs(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	keyValuePairsAfterSplit := page.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("O")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithKeyValuePairsInSiblingWithEvenNumberOfKeyValuePairs(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13, 14},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	keyValuePairsAfterSplit := siblingPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the sibling page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithKeyValuePairsInSiblingWithOddNumberOfKeyValuePairs(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	keyValuePairsAfterSplit := siblingPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("J")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the sibling page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithChildPageIdsWithEvenNumberOfChildPageIds(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	childPageIdsAfterSplit := page.childPageIds
	expected := []int{12, 13}

	if !reflect.DeepEqual(expected, childPageIdsAfterSplit) {
		t.Fatalf("Expected child page ids in the page after split to be %v, received %v", expected, childPageIdsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithChildPageIdsInSiblingPageWithEvenNumberOfChildPageIds(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	childPageIdsAfterSplit := siblingPage.childPageIds
	expected := []int{10, 11}

	if !reflect.DeepEqual(expected, childPageIdsAfterSplit) {
		t.Fatalf("Expected child page ids in the sibling page after split to be %v, received %v", expected, childPageIdsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithChildPageIdsWithOddNumberOfChildPageIds(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13, 14},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	childPageIdsAfterSplit := page.childPageIds
	expected := []int{13, 14}

	if !reflect.DeepEqual(expected, childPageIdsAfterSplit) {
		t.Fatalf("Expected child page ids in the page after split to be %v, received %v", expected, childPageIdsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithChildPageIdsInSiblingPageWithOddNumberOfChildPageIds(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13, 14},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	childPageIdsAfterSplit := siblingPage.childPageIds
	expected := []int{10, 11, 12}

	if !reflect.DeepEqual(expected, childPageIdsAfterSplit) {
		t.Fatalf("Expected child page ids in the sibling page after split to be %v, received %v", expected, childPageIdsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithAKeyValuePairAddedToParent(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13, 14},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5, 6}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	parentPageKeyValuePairs := parentPage.keyValuePairs
	expected := []KeyValuePair{{key: []byte("S")}, {key: []byte("O")}}

	if !reflect.DeepEqual(expected, parentPageKeyValuePairs) {
		t.Fatalf("Expected parent page to contain key value pairs after split to be %v, received %v", expected, parentPageKeyValuePairs)
	}
}

func TestSplitsANonLeafPageWithKeyValuePairsInParent(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	keyValuePairsAfterSplit := parentPage.AllKeyValuePairs()
	expected := []KeyValuePair{{key: []byte("S")}, {key: []byte("O")}}

	if !reflect.DeepEqual(expected, keyValuePairsAfterSplit) {
		t.Fatalf("Expected key value pairs in the parent page after split to be %v, received %v", expected, keyValuePairsAfterSplit)
	}
}

func TestSplitsANonLeafPageWithChildPageIdAdddedToParent(t *testing.T) {
	page := &Page{
		id:            5,
		keyValuePairs: []KeyValuePair{{key: []byte("J")}, {key: []byte("L")}, {key: []byte("O")}, {key: []byte("Q")}},
		childPageIds:  []int{10, 11, 12, 13},
	}
	parentPage := NewPage(100)
	parentPage.childPageIds = []int{5}
	parentPage.keyValuePairs = []KeyValuePair{{key: []byte("S")}}

	siblingPage := NewPage(200)

	_, _ = page.split(parentPage, siblingPage, 1)

	childPageIdsOfParent := parentPage.childPageIds
	expected := []int{5, 200}

	if !reflect.DeepEqual(expected, childPageIdsOfParent) {
		t.Fatalf("Expected parent page to contain child page ids after split to be %v, received %v", expected, childPageIdsOfParent)
	}
}

func TestReturnsTheSizeOfALeafPage(t *testing.T) {
	page := &Page{
		id:            0,
		keyValuePairs: []KeyValuePair{{key: []byte("A"), value: []byte("Database")}},
	}
	size := page.size()
	expected := 13

	if expected != size {
		t.Fatalf("Expected leaf page size to be %v, received %v", expected, size)
	}
}

func TestReturnsTheSizeOfANonLeafPage(t *testing.T) {
	page := &Page{
		id:            0,
		keyValuePairs: []KeyValuePair{{key: []byte("A")}},
		childPageIds:  []int{10, 11},
	}
	size := page.size()
	expected := 14

	if expected != size {
		t.Fatalf("Expected non-leaf page size to be %v, received %v", expected, size)
	}
}
