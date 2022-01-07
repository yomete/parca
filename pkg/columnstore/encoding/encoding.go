package encoding

type Encoding uint64

const (
	PlainEncoding Encoding = iota
	RLEEncoding
	DictionaryEncoding
	DictionaryRLEEncoding
)
