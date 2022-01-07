package encoding

import (
	"errors"

	"github.com/parca-dev/parca/pkg/columnstore/types"
)

type Value interface {
}

type Plain struct {
	typ    types.Type
	values []Value
	count  int
}

func NewPlain(typ types.Type, maxCount int) *Plain {
	return &Plain{
		typ:    typ,
		values: make([]Value, maxCount),
		count:  0,
	}
}

func (c *Plain) Insert(index int, v Value) (int, error) {
	if index < 0 || index > len(c.values) {
		return -1, errors.New("index out of range")
	}

	copy(c.values[index+1:], c.values[index:])
	c.values[index] = v

	c.count++
	return c.count, nil
}
