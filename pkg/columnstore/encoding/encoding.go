package encoding

type Encoding uint64

const (
	Plain Encoding = iota
	RLE
	Dictionary
	DictionaryRLE
)
