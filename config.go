package common_io

//CallbackFunc is the function prototype for kafka callback on message received
type CallbackFunc func(msg []byte)

//Config to be used on io_common Setup
type Config struct {
	ModuleName struct {
		Value string
	}
	Topics map[string]CallbackFunc
	Kafka  struct {
		BrokerList []string
		MaxRetry   int
	}
	Zookeeper struct {
		AddrList []string
		MaxRetry int
	}
}
