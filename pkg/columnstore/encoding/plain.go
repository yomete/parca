package encoding

import (
	"errors"

	"github.com/parca-dev/parca/pkg/columnstore/types"
)

type Plain struct {
	typ    types.Type
	values []types.Value
	count  int
}

func NewPlain(typ types.Type, maxCount int) *Plain {
	return &Plain{
		typ:    typ,
		values: make([]types.Value, maxCount),
		count:  0,
	}
}

func (c *Plain) Insert(index int, v types.Value) (int, error) {
	if index < 0 || index > len(c.values) {
		return -1, errors.New("index out of range")
	}

	copy(c.values[index+1:], c.values[index:])
	c.values[index] = v

	c.count++
	return c.count, nil
}

type IndexRange struct {
	Start int
	End   int
}

func (c *Plain) Find(v types.Value) (IndexRange, error) {
	indexRange := IndexRange{
		Start: -1,
		End:   -1,
	}
	for i := 0; i < len(c.values); i++ {
		if c.values[i].Less(v) {
			indexRange.Start = i + 1
			continue
		}
		// Something wrong here
		if indexRange.Start != -1 && c.values[i].Equal(v) {
			indexRange.End = i
		}
	}

	return indexRange, nil
}
