package aws

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *Client) DownloadFile(
	ctx context.Context, bucket string, objectKey string, w io.WriterAt, partMiBs int64, concurrency int,
) error {
	client := s3.NewFromConfig(c.c)

	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
		d.Concurrency = concurrency
	})

	_, err := downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) ListFiles(
	ctx context.Context, bucket string, prefix string,
) (files []string, err error) {
	client := s3.NewFromConfig(c.c)

	var continuationToken *string = nil
	for {
		res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return files, err
		}
		for _, obj := range res.Contents {
			if obj.Key != nil && *obj.Key != "" {
				files = append(files, *obj.Key)
			}
		}

		if res.ContinuationToken == nil {
			return files, nil
		}

		continuationToken = res.ContinuationToken
	}
}
