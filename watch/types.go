package watch

import "strings"

const (
	CREATED  EventType = 1 << iota // 1 (binary: 001)
	MODIFIED                       // 2 (binary: 010)
	DELETED                        // 4 (binary: 100)
	ALL      = -1
)

type EventType int

func (e EventType) Is(flag EventType) bool {
	return e&flag != 0
}

func (e EventType) String() string {
	if e == -1 {
		return "ALL"
	}

	if e == 0 {
		return "NONE"
	}

	var flags []string
	if e.Is(CREATED) {
		flags = append(flags, "CREATED")
	}
	if e.Is(MODIFIED) {
		flags = append(flags, "MODIFIED")
	}
	if e.Is(DELETED) {
		flags = append(flags, "DELETED")
	}

	if len(flags) == 0 {
		return "INVALID"
	}

	return strings.Join(flags, ",")
}
