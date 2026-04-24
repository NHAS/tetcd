package kind

type Kind int

const (
	KindIntermediate Kind = iota // just a path segment, no value
	KindSimple                   // terminal key=value
	KindMap                      // prefix, owns all children
)
