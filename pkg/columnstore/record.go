package columnstore

import "github.com/parca-dev/parca/pkg/columnstore/types"

type RecordColumnData struct {
	Name  string
	Value types.Value
}

type Record struct {
	Data []RecordColumnData
}
