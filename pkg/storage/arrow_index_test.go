package storage

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

func TestLabelIndex(t *testing.T) {
	li := NewLabelIndex()

	lset := labels.FromStrings("foo", "bar")
	lsetID := lset.Hash()

	require.Equal(t, lsetID, li.Add(lset))

	bitmap, err := li.Postings("foo", "bar")
	require.NoError(t, err)
	require.Equal(t, []uint64{lsetID}, bitmap.ToArray())

	require.Equal(t, labels.Labels{}, li.LabelsForID(1)) // not found
	require.Equal(t,
		labels.Labels{{Name: "foo", Value: "bar"}},
		li.LabelsForID(lsetID),
	)
}
