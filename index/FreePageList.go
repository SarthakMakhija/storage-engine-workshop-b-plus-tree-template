package index

type FreePageList struct {
	pageIds []int
}

func InitializeFreePageList(startingPageId int, pageCount int) *FreePageList {
	freePageList := &FreePageList{}
	pageId := startingPageId

	for index := 1; index <= pageCount; index++ {
		freePageList.pageIds = append(freePageList.pageIds, pageId)
		pageId = pageId + 1
	}
	return freePageList
}

func (freePageList *FreePageList) allocateAndUpdate(pages int) int {
	firstFreePageId, remainingFreePageIds := freePageList.allocateContiguous(pages)
	freePageList.pageIds = remainingFreePageIds
	return firstFreePageId
}

func (freePageList *FreePageList) allocateContiguous(pages int) (int, []int) {
	if len(freePageList.pageIds) < pages {
		return -1, freePageList.pageIds
	} else if pages == 1 {
		return freePageList.pageIds[0], freePageList.pageIds[1:]
	}

	startingIndex, endIndex := 0, 0
	for ; startingIndex < len(freePageList.pageIds); startingIndex++ {
		endIndex = startingIndex + (pages - 1)
		if endIndex < len(freePageList.pageIds) && freePageList.pageIds[endIndex] == freePageList.pageIds[startingIndex]+(pages-1) {
			break
		}
	}

	if startingIndex >= len(freePageList.pageIds) || endIndex >= len(freePageList.pageIds) {
		return -1, freePageList.pageIds
	}

	firstFreePageId := freePageList.pageIds[startingIndex]
	freePageList.pageIds = append(freePageList.pageIds[:startingIndex], freePageList.pageIds[endIndex+1:]...)
	return firstFreePageId, freePageList.pageIds
}
