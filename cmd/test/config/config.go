package config

import "github.com/NHAS/tetcd/cmd/test/another"

type somethingElse struct {
	Test    string
	Toaster string
}

type ServerConfig struct {
	somethingElse

	Host string
	Port int
}

type NestedExternal struct {
	another.SomeType
}

type CompressedStruct struct {
	Noot string

	Toaster string
}

type TLSConfig struct {
	CertFile string
	KeyFile  string

	NestedInTls struct {
		Number    int
		Something string
		Fronk     string

		DoublyNested struct {
			Arghh string
		}
	}

	Ahh another.SomeType

	Groups map[string][]string
}

type InnerMap struct {
	Ahhh int
}

type Config struct {
	Server ServerConfig
	TLS    TLSConfig

	Hello NestedExternal

	Dummy struct {
		Something5 string
	}

	SomethingElse struct {
		another.SomeType
		Extra   string
		Methods []string
	}

	CompressMe CompressedStruct `tetcd:"compress"`

	Name   string
	Labels map[string]string

	MapTest map[string]InnerMap

	Tags []string `tetcd:"compress"`

	Methods1 []string

	EnumMap map[string]Enum

	// should be skipped
	ignored      string
	SkippedField string `tetcd:"-"`
}

type Enum string

const (
	Thing1 Enum = "woop"
	Thing2 Enum = "shoop"
)
