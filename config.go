package common_io

import (
	"fmt"
	"gopkg.in/gcfg.v1"
)

//CallbackFunc is the function prototype for kafka callback on message received
type CallbackFunc func(msg []byte)

//Config to be used on io_common Setup
type Config struct {
	ModuleName string
	Topics     map[string]CallbackFunc
	Kafka      struct {
		BrokerList []string
	}
	Zookeeper struct {
		AddrList []string
	}
}

// NewConf returns the Conf struct populated with the common_io_config.gcfg info
func LoadConfig() (*Config, error) {
	cfg := Config{}
	err := gcfg.ReadFileInto(&cfg, "common_io_config.gcfg")

	if err != nil {
		fmt.Println(">> Error while trying to read config from file\n", err)
		return nil, err
	}

	return &cfg, nil
}
