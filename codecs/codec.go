package codecs

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	EncodeRaw(any) ([]byte, error)
	Decode([]byte) (T, error)
	DecodeToPointer(data []byte, ptr *T) error
}
