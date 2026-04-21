package codecs

type NoopCodec[T any] struct{}

func NewNoopCodec[T any]() NoopCodec[T] {
	return NoopCodec[T]{}
}

func (NoopCodec[T]) Encode(val T) ([]byte, error) {
	return nil, nil
}

func (NoopCodec[T]) Decode(data []byte) (T, error) {
	var val T
	return val, nil
}
