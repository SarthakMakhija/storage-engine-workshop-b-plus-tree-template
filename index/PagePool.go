package index

type PagePool struct {
	indexFile *IndexFile
	pageSize  int
	pageCount int
}

func NewPagePool(indexFile *IndexFile, options Options) *PagePool {
	pagePool := &PagePool{
		indexFile: indexFile,
	}
	pagePool.pageSize = options.PageSize
	pagePool.pageCount = pagePool.numberOfPages()

	return pagePool
}

func (pagePool *PagePool) Allocate(pages int) (int, error) {
	nextPageId := pagePool.pageCount
	targetSize := pagePool.indexFile.size + int64(pages*pagePool.pageSize)
	if err := pagePool.indexFile.ResizeTo(targetSize); err != nil {
		return 0, err
	}
	pagePool.pageCount = pagePool.numberOfPages()
	return nextPageId, nil
}

func (pagePool PagePool) Read(pageId int) (*Page, error) {
	bytes, err := pagePool.indexFile.readFrom(pagePool.offsetOf(pageId), pagePool.pageSize)
	if err != nil {
		return nil, err
	}
	page := &Page{id: pageId}
	page.UnMarshalBinary(bytes)
	return page, nil
}

func (pagePool *PagePool) Write(page *Page) {
	pagePool.indexFile.writeAt(pagePool.offsetOf(page.id), page.MarshalBinary())
}

func (pagePool PagePool) offsetOf(pageId int) int64 {
	return int64(pagePool.pageSize * pageId)
}

func (pagePool PagePool) ContainsZeroPages() bool {
	return pagePool.pageCount == 0
}

func (pagePool *PagePool) Close() error {
	return pagePool.indexFile.Close()
}

func (pagePool PagePool) numberOfPages() int {
	return int(pagePool.indexFile.size) / pagePool.pageSize
}
