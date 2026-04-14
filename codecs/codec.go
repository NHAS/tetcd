package codecs

import (
	"encoding/json"
)

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

type JSONCodec[T any] struct{}

func NewJsonCodec[T any]() JSONCodec[T] {
	return JSONCodec[T]{}
}

func (JSONCodec[T]) Encode(val T) ([]byte, error) {
	return json.Marshal(val)
}

func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var val T
	err := json.Unmarshal(data, &val)
	return val, err
}
