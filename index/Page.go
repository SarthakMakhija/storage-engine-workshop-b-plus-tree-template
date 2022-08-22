package index

import (
	"b+tree/index/schema"
	"bytes"
	"math"
	"sort"
)

const (
	LeafPage    = uint8(0x0)
	NonLeafPage = uint8(0x01)
)

type Page struct {
	id            int
	keyValuePairs []KeyValuePair
	childPageIds  []int
}

type DirtyPage struct {
	page *Page
}

func NewPage(id int) *Page {
	return &Page{
		id: id,
	}
}

func (page Page) Get(key []byte) (int, bool) {
	return page.binarySearch(key)
}

func (page Page) GetKeyValuePairAt(index int) KeyValuePair {
	return page.keyValuePairs[index]
}

func (page Page) MarshalBinary() []byte {
	if page.isLeaf() {
		buffer, _ := page.toPersistentLeafPage().Marshal(nil)
		return buffer
	} else {
		buffer, _ := page.toPersistentNonLeafPage().Marshal(nil)
		return buffer
	}
}

func (page Page) toPersistentLeafPage() *schema.PersistentLeafPage {
	persistentKeyValuePairs := make([]schema.PersistentKeyValuePair, len(page.keyValuePairs))
	for index, keyValuePair := range page.keyValuePairs {
		persistentKeyValuePairs[index] = keyValuePair.toPersistentKeyValuePair()
	}
	return &schema.PersistentLeafPage{
		PageType: LeafPage,
		Pairs:    persistentKeyValuePairs,
	}
}

func (page Page) toPersistentNonLeafPage() *schema.PersistentNonLeafPage {
	persistentKeyValuePairs := make([]schema.PersistentKeyValuePair, len(page.keyValuePairs))
	childPageIds := make([]uint32, len(page.childPageIds))

	for index, keyValuePair := range page.keyValuePairs {
		persistentKeyValuePairs[index] = keyValuePair.toPersistentKeyValuePair()
	}
	for index, childPageId := range page.childPageIds {
		childPageIds[index] = uint32(childPageId)
	}
	return &schema.PersistentNonLeafPage{
		PageType:     NonLeafPage,
		Pairs:        persistentKeyValuePairs,
		ChildPageIds: childPageIds,
	}
}

func (page *Page) UnMarshalBinary(buffer []byte) {
	if buffer[0]&NonLeafPage == 0 {
		persistentLeafPage := schema.PersistentLeafPage{}
		_, _ = persistentLeafPage.Unmarshal(buffer)

		for _, persistentKeyValuePair := range persistentLeafPage.Pairs {
			page.keyValuePairs = append(
				page.keyValuePairs,
				KeyValuePair{
					key: persistentKeyValuePair.Key, value: persistentKeyValuePair.Value,
				},
			)
		}
	} else {
		persistentNonLeafPage := schema.PersistentNonLeafPage{}
		_, _ = persistentNonLeafPage.Unmarshal(buffer)

		for _, persistentKeyValuePair := range persistentNonLeafPage.Pairs {
			page.keyValuePairs = append(
				page.keyValuePairs,
				KeyValuePair{
					key: persistentKeyValuePair.Key, value: persistentKeyValuePair.Value,
				},
			)
		}
		for _, persistentChildPageId := range persistentNonLeafPage.ChildPageIds {
			page.childPageIds = append(
				page.childPageIds,
				int(persistentChildPageId),
			)
		}
	}
}

func (page Page) size() int {
	if page.isLeaf() {
		return int(page.toPersistentLeafPage().Size())
	} else {
		return int(page.toPersistentNonLeafPage().Size())
	}
}

func (page Page) binarySearch(key []byte) (int, bool) {
	index := sort.Search(len(page.keyValuePairs), func(index int) bool {
		if bytes.Compare(key, page.keyValuePairs[index].key) < 0 {
			return true
		}
		return false
	})
	if index > 0 && bytes.Compare(page.keyValuePairs[index-1].key, key) == 0 {
		return index - 1, true
	}
	return index, false
}

func (page Page) isLeaf() bool {
	return len(page.childPageIds) == 0
}

func (page *Page) insertAt(index int, keyValuePair KeyValuePair) DirtyPage {
	page.keyValuePairs = append(page.keyValuePairs, KeyValuePair{})

	copy(page.keyValuePairs[index+1:], page.keyValuePairs[index:])
	if page.isLeaf() {
		page.keyValuePairs[index] = keyValuePair
	} else {
		page.keyValuePairs[index] = KeyValuePair{key: keyValuePair.key}
	}
	return DirtyPage{page: page}
}

func (page *Page) updateAt(index int, keyValuePair KeyValuePair) DirtyPage {
	page.keyValuePairs[index] = keyValuePair
	return DirtyPage{page: page}
}

func (page *Page) insertChildAt(index int, childPage *Page) DirtyPage {
	page.childPageIds = append(page.childPageIds, 0)
	copy(page.childPageIds[index+1:], page.childPageIds[index:])
	page.childPageIds[index] = childPage.id

	return DirtyPage{page: page}
}

func (page *Page) split(parentPage *Page, siblingPage *Page, index int) ([]DirtyPage, error) {
	dirtyPages := []DirtyPage{{page: page}, {page: siblingPage}, {page: parentPage}}

	if page.isLeaf() {
		pageKeyValuePairs := page.AllKeyValuePairs()
		siblingPage.keyValuePairs = append(siblingPage.keyValuePairs, page.keyValuePairs[len(pageKeyValuePairs)/2:]...)
		page.keyValuePairs = page.keyValuePairs[:len(pageKeyValuePairs)/2]

		dirtyPages = append(dirtyPages, parentPage.insertChildAt(index+1, siblingPage))
		dirtyPages = append(dirtyPages, parentPage.insertAt(index, siblingPage.keyValuePairs[0]))
	} else {
		parentKey := page.keyValuePairs[len(page.AllKeyValuePairs())/2]

		siblingPage.keyValuePairs = append(siblingPage.keyValuePairs, page.keyValuePairs[:len(page.AllKeyValuePairs())/2]...)
		page.keyValuePairs = page.keyValuePairs[len(page.AllKeyValuePairs())/2+1:]

		if math.Mod(float64(len(page.childPageIds)), 2) != 0 {
			siblingPage.childPageIds = append(siblingPage.childPageIds, page.childPageIds[:len(page.childPageIds)/2+1]...)
			page.childPageIds = page.childPageIds[len(page.childPageIds)/2+1:]
		} else {
			siblingPage.childPageIds = append(siblingPage.childPageIds, page.childPageIds[:len(page.childPageIds)/2]...)
			page.childPageIds = page.childPageIds[len(page.childPageIds)/2:]
		}

		dirtyPages = append(dirtyPages, parentPage.insertChildAt(index, siblingPage))
		dirtyPages = append(dirtyPages, parentPage.insertAt(index, parentKey))
	}
	return dirtyPages, nil
}

func (page *Page) AllKeyValuePairs() []KeyValuePair {
	return page.keyValuePairs
}
