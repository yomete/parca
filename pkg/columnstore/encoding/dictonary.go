package encoding

import (
	"fmt"

	"github.com/parca-dev/parca/pkg/columnstore/types"
	"github.com/parca-dev/parca/pkg/storage/chunkenc"
)

type DictionaryRLE struct {
	typ      types.Type
	rev      map[int64]types.Value
	dict     map[types.Value]int64
	values   *chunkenc.RLEChunk
	appender chunkenc.Appender
	count    int
}

func NewDictionaryRLE(typ types.Type, maxCount int) *DictionaryRLE {
	enc := chunkenc.NewRLEChunk()
	app, _ := enc.Appender()
	return &DictionaryRLE{
		typ:      typ,
		values:   enc,
		appender: app,
		count:    0,

		dict: map[types.Value]int64{},
		rev:  map[int64]types.Value{},
	}
}

func (c *DictionaryRLE) String() string {
	it := c.values.Iterator(nil)
	var s string
	for i := 0; it.Next(); i++ {
		val := c.rev[it.At()]
		s = s + fmt.Sprintf("%v,", val)
	}
	s = s + "\n"
	return s
}

func (c *DictionaryRLE) Insert(index int, v types.Value) (int, error) {

	// lookup dictionary index
	val, ok := c.dict[v]
	if !ok { // new element in dict
		val = int64(len(c.dict))
		c.dict[v] = val
		c.rev[val] = v
	}

	c.appender.(*chunkenc.RLEAppender).Insert(uint16(index), int64(val))

	c.count++
	return c.count, nil
}

func (c *DictionaryRLE) Find(v types.Value) (IndexRange, error) {
	indexRange := IndexRange{
		Start: -1,
		End:   -1,
	}

	_, ok := c.dict[v]
	if !ok {
		return indexRange, nil
	}

	it := c.values.Iterator(nil)
	for i := 0; it.Next(); i++ {
		val := c.rev[it.At()]
		if val.Less(v) {
			indexRange.Start = i + 1
			continue
		}

		if indexRange.Start != -1 && val.Equal(v) {
			indexRange.End = i
		}
	}

	return indexRange, nil
}
