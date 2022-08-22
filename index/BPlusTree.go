package index

type BPlusTree struct {
	fileName      string
	pagePool      *PagePool
	pageHierarchy *PageHierarchy
	freePageList  *FreePageList
}

const metaPageCount = 1
const rootPageCount = 1

func CreateBPlusTree(options Options) (*BPlusTree, error) {
	indexFile, err := OpenIndexFile(options)
	pagePool := NewPagePool(indexFile, options)

	if err != nil {
		return nil, err
	}
	tree := &BPlusTree{
		fileName: options.FileName,
		pagePool: pagePool,
	}
	if err := tree.create(options); err != nil {
		return nil, err
	}
	tree.pageHierarchy = NewPageHierarchy(pagePool, options.AllowedPageOccupancyPercentage, tree.freePageList)
	return tree, nil
}

func (tree BPlusTree) Put(key, value []byte) error {
	if err := tree.pageHierarchy.Put(KeyValuePair{key: append([]byte(nil), key...), value: append([]byte(nil), value...)}); err != nil {
		return err
	}
	return nil
}

func (tree BPlusTree) Get(key []byte) GetResult {
	return tree.pageHierarchy.Get(key)
}

func (tree *BPlusTree) Close() error {
	return tree.pagePool.Close()
}

func (tree *BPlusTree) create(options Options) error {
	if tree.pagePool.ContainsZeroPages() {
		return tree.initialize(options)
	}
	return nil
}

func (tree *BPlusTree) initialize(options Options) error {
	_, err := tree.pagePool.Allocate(metaPageCount + rootPageCount + options.PreAllocatedPagePoolSize)
	if err != nil {
		return err
	}
	tree.freePageList = InitializeFreePageList(metaPageCount+rootPageCount, options.PreAllocatedPagePoolSize)
	return nil
}
