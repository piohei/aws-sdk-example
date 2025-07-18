package main

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Verbose         bool

	DownloadBufferSizeMiBs int
	DownloadPartMiBs       int64
	DownloadConcurrency    int
	SnapshotsS3Bucket      string
	AWSClientProfile       string
}

func readConfig() (config Config, err error) {
	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	viper.SetConfigType("toml")

	viper.SetDefault("Verbose", false)
	viper.SetDefault("DownloadBufferSizeMiBs", 1024)
	viper.SetDefault("DownloadPartMiBs", 64)
	viper.SetDefault("DownloadConcurrency", 4)

	err = viper.BindEnv("Verbose", "GETH_WRAPPER_VERBOSE")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}
	err = viper.BindEnv("DownloadBufferSizeMiBs", "GETH_WRAPPER_DOWNLOAD_BUFFER_SIZE_MIBS")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}
	err = viper.BindEnv("DownloadPartMiBs", "GETH_WRAPPER_DOWNLOAD_PART_MIBS")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}
	err = viper.BindEnv("DownloadConcurrency", "GETH_WRAPPER_DOWNLOAD_CONCURRENCY")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}
	err = viper.BindEnv("SnapshotsS3Bucket", "GETH_WRAPPER_SNAPSHOTS_S3_BUCKET")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}
	err = viper.BindEnv("AWSClientProfile", "GETH_WRAPPER_AWS_CLIENT_PROFILE")
	if err != nil {
		return config, fmt.Errorf("error while binding env config: %v", err)
	}

	err = viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return config, fmt.Errorf("error while reading config: %v", err)
		}
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return config, fmt.Errorf("error while unmarshalling config: %v", err)
	}

	if config.SnapshotsS3Bucket == "" {
		return config, fmt.Errorf("SnapshotsS3Bucket must be provided")
	}

	return
}
