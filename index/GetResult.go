package index

type GetResult struct {
	KeyValuePair KeyValuePair
	index        int
	page         *Page
	found        bool
	Err          error
}

func NewKeyAvailableGetResult(pair KeyValuePair, index int, page *Page) GetResult {
	return GetResult{
		KeyValuePair: pair,
		index:        index,
		page:         page,
		found:        true,
		Err:          nil,
	}
}

func NewKeyMissingGetResult(index int, page *Page) GetResult {
	return GetResult{
		KeyValuePair: KeyValuePair{},
		index:        index,
		page:         page,
		found:        false,
		Err:          nil,
	}
}

func NewFailedGetResult(err error) GetResult {
	return GetResult{
		found: false,
		Err:   err,
	}
}
