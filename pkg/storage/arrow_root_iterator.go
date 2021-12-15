package storage

import (
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/math"
	"github.com/prometheus/prometheus/pkg/labels"
)

type ArrowRootSeries struct {
	schema     *arrow.Schema
	meta       arrow.Metadata
	records    []array.Record
	labelSetID uint64
	mint       int64
	maxt       int64
}

func (ars *ArrowRootSeries) Labels() labels.Labels {
	return labels.Labels{{
		Name:  "id",
		Value: strconv.FormatUint(ars.labelSetID, 10),
	}}
}

func (ars *ArrowRootSeries) Iterator() ProfileSeriesIterator {
	return &ArrowRootSeriesIterator{
		meta:    ars.meta,
		records: ars.records,
		numRead: -1,
	}
}

type ArrowRootSeriesIterator struct {
	meta    arrow.Metadata
	records []array.Record

	numRead int
	err     error
}

func (it *ArrowRootSeriesIterator) Err() error {
	return it.err
}

func (it *ArrowRootSeriesIterator) Next() bool {
	it.numRead++
	return it.numRead < len(it.records)
}

func (it *ArrowRootSeriesIterator) At() InstantProfile {
	r := it.records[it.numRead]

	profileMeta := InstantProfileMeta{}

	meta := r.Schema().Metadata()
	{
		t := meta.Values()[meta.FindKey(timestampMeta)]
		timestamp, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			it.err = err
			return nil
		}
		profileMeta.Timestamp = timestamp
	}
	{
		d := meta.Values()[meta.FindKey(durationMeta)]
		duration, err := strconv.ParseInt(d, 10, 64)
		if err != nil {
			it.err = err
			return nil
		}
		profileMeta.Duration = duration
	}
	{
		p := meta.Values()[meta.FindKey(periodMeta)]
		period, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			it.err = err
			return nil
		}
		profileMeta.Period = period
	}

	profileMeta.SampleType = ValueTypeFromString(
		meta.Values()[meta.FindKey(sampleTypeMeta)],
	)
	profileMeta.PeriodType = ValueTypeFromString(
		meta.Values()[meta.FindKey(periodTypeMeta)],
	)

	// This uses arrow's functionality to sum all values of the record
	data := array.NewInt64Data(r.Column(colValue).Data())
	cumulative := math.Int64.Sum(data)

	return &FlatProfile{
		Meta: profileMeta,
		samples: map[string]*Sample{
			"": {Value: cumulative},
		},
	}
}
