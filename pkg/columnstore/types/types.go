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
