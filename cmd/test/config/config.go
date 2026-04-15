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

	Name   string
	Labels map[string]string
	Tags   []string `tetcd:"compress"`

	Methods1 []string

	// should be skipped
	ignored      string
	SkippedField string `tetcd:"-"`
}
