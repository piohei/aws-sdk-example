package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/config"
)

type Client struct {
	c aws.Config
}

func NewClient(ctx context.Context, profile string) (*Client, error) {
	// Load the Shared AWS Configuration (~/.aws/config)
	c, err := func() (aws.Config, error) {
		if profile != "" {
			return config.LoadDefaultConfig(
				ctx,
				config.WithSharedConfigProfile(profile),
			)
		} else {
			return config.LoadDefaultConfig(ctx)
		}
	}()
	if err != nil {
		return nil, err
	}

	return &Client{
		c: c,
	}, nil
}
