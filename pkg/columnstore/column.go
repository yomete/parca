package columnstore

import "github.com/parca-dev/parca/pkg/columnstore/types"

type ColumnData interface {
	Insert(index int, v types.Value) int
}

type Column struct {
	Name       string
	ColumnData ColumnData
}
