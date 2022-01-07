package types

type Type uint64

const (
	String Type = iota
	Uint64
	Int64
	UUID
)

type Value struct {
	Data interface{}
}

func (v *Value) Less(other Value) bool {
	if v.Data == nil && other.Data == nil {
		return false
	}
	if v.Data == nil && other.Data != nil {
		return false
	}
	if v.Data != nil && other.Data == nil {
		return true
	}

	switch v.Data.(type) {
	case string:
		return v.Data.(string) < other.Data.(string)
	case int64:
		return v.Data.(int64) < other.Data.(int64)
	case uint64:
		return v.Data.(uint64) < other.Data.(uint64)
	default:
		panic("unsupported type")
	}
}

func (v *Value) Equal(other Value) bool {
	if v.Data == nil && other.Data == nil {
		return true
	}
	if v.Data == nil && other.Data != nil {
		return false
	}
	if v.Data != nil && other.Data == nil {
		return false
	}

	switch v.Data.(type) {
	case string:
		return v.Data.(string) == other.Data.(string)
	case int64:
		return v.Data.(int64) == other.Data.(int64)
	case uint64:
		return v.Data.(uint64) == other.Data.(uint64)
	default:
		panic("unsupported type")
	}
}
