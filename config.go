package main

import (
	"fmt"
	"os"

	"github.com/naoina/toml"
	"github.com/thbourlove/outflow/client"
	"github.com/thbourlove/outflow/httpd"
)

type Config struct {
	Upstreams client.Config `toml:"upstreams"`
	Httpd     httpd.Config  `toml:"httpd"`
}

func ParseConfig(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %v", path, err)
	}
	defer f.Close()

	cfg := &Config{}
	return cfg, toml.NewDecoder(f).Decode(cfg)
}
