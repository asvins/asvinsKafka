package common_io

//Config to be used on io_common Setup
type Config struct {
	ModuleName struct {
		Value string
	}
	Kafka struct {
		BrokerList []string
		MaxRetry   int
	}
	Zookeeper struct {
		AddrList []string
		MaxRetry int
	}

	Deadletters struct {
		Frequency int
	}
}
