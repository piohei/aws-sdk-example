package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/piohei/aws-sdk-example/internal/aws"
	"github.com/piohei/aws-sdk-example/internal/tar"

	log "github.com/sirupsen/logrus"
)

func setLogLevel(verbose bool) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	if verbose {
		log.SetLevel(log.TraceLevel)
	}
}

func main() {
	config, err := readConfig()
	if err != nil {
		fmt.Printf("Failed to read configuration: %v\n", err)
		os.Exit(1)
	}

	setLogLevel(config.Verbose)

	log.Infof("Loaded config: %+v", config)

	ctx := context.Background()
	awsClient, err := aws.NewClient(ctx, config.AWSClientProfile)
	if err != nil {
		log.Errorf("Error while creating aws client: %v", err)
		os.Exit(1)
	}

	files, err := awsClient.ListFiles(ctx, config.SnapshotsS3Bucket, "")
	if err != nil {
		return err
	}

	latestFile := slices.Max(files)

	log.Infof("Downloading latest file (%v) from bucket (%v).", latestFile, config.SnapshotsS3Bucket)

	err = tar.ExtractFromS3(
		ctx, awsClient,
		config.SnapshotsS3Bucket,
		latestSnapshot,
		"./",
		config.DownloadBufferSizeMiBs,
		config.DownloadPartMiBs,
		config.DownloadConcurrency,
	)
	if err != nil {
		log.Errorf("Error downloading file: %v", err)
		os.Exit(1)
	}
}