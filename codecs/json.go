package codecs

import (
	"bytes"
	"encoding/json"
)

type JSONCodec[T any] struct{}

func NewJsonCodec[T any]() JSONCodec[T] {
	return JSONCodec[T]{}
}

func (JSONCodec[T]) Encode(val T) ([]byte, error) {
	return json.Marshal(val)
}

func (JSONCodec[T]) EncodeRaw(val any) ([]byte, error) {
	return json.Marshal(val)
}

func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var val T

	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.DisallowUnknownFields()

	err := dec.Decode(&val)
	if err != nil {
		return val, err
	}

	return val, nil
}
