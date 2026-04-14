package config

type ServerConfig struct {
	Host string
	Port int
}

type TLSConfig struct {
	CertFile string
	KeyFile  string
}

type Config struct {
	Server ServerConfig
	TLS    TLSConfig

	Name   string
	Labels map[string]string
	Tags   []string `tetcd:"compress"`

	// should be skipped
	ignored      string
	SkippedField string `tetcd:"-"`
}
