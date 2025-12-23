package main

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

func main() {

	type Config struct {
		Server struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		} `yaml:"server"`
		Debug bool `yaml:"debug"`
	}

	data, err := os.ReadFile("./test.yaml")
	if err != nil {
		panic(err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", cfg.Server)
}
