package encoding

import (
	"fmt"
	"testing"

	"github.com/parca-dev/parca/pkg/columnstore/types"
	"github.com/stretchr/testify/require"
)

func Test_DictionaryRLE(t *testing.T) {
	p := NewDictionaryRLE(types.String, 10)

	i := 0
	count, err := p.Insert(i, types.Value{Data: "test1"})
	i++
	require.NoError(t, err)
	require.Equal(t, i, count)

	for ; i < 4; i++ {
		count, err = p.Insert(i, types.Value{Data: "test2"})
		require.NoError(t, err)
		require.Equal(t, i+1, count)
	}

	count, err = p.Insert(4, types.Value{Data: "test3"})
	i++
	require.NoError(t, err)
	require.Equal(t, i, count)

	indexRange, err := p.Find(types.Value{Data: "test2"})
	require.NoError(t, err)
	require.Equal(t, IndexRange{Start: 1, End: 3}, indexRange)
}

func Test_DictionaryRLE_Insert(t *testing.T) {
	p := NewDictionaryRLE(types.String, 10)

	i := 0
	count, err := p.Insert(i, types.Value{Data: "test1"})
	i++
	require.NoError(t, err)
	require.Equal(t, i, count)

	count, err = p.Insert(1, types.Value{Data: "test3"})
	i++
	require.NoError(t, err)
	require.Equal(t, i, count)

	count, err = p.Insert(1, types.Value{Data: "test2"})
	i++
	require.NoError(t, err)
	require.Equal(t, i, count)

	fmt.Println(p)
}
