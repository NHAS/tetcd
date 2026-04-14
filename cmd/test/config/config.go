package config

type somethingElse struct {
	Test    string
	Toaster string
}

type ServerConfig struct {
	somethingElse

	Host string
	Port int
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

	Groups map[string][]string
}

type Config struct {
	Server ServerConfig
	TLS    TLSConfig

	Dummy struct {
		Something5 string
	}

	Name   string
	Labels map[string]string
	Tags   []string `tetcd:"compress"`

	// should be skipped
	ignored      string
	SkippedField string `tetcd:"-"`
}
