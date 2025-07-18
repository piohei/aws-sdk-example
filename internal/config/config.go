package config

type Config struct {
	Verbose bool

	Environment   string
	Team          string
	Namespace     string
	Name          string
	KeepLastNDays int

	AWSProfile    string
	AWSBucketName string
}

func New() Config {
	return Config{}
}
