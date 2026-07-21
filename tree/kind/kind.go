package kind

type Kind int

const (
	Intermediate Kind = iota // just a path segment, no value
	Simple                   // terminal key=value
	Map                      // prefix, owns all children
	Ignored                  // prefix, owns all children and excludes them from any planned change

)
