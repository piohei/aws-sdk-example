package tar

import (
	"archive/tar"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/piohei/aws-sdk-example/internal/aws"
)

type Stats struct {
	staredAt   time.Time
	totalBytes int64
	wrapped    *ringBuffer
}

func (s *Stats) WriteAt(p []byte, off int64) (n int, err error) {
	written, err := s.wrapped.WriteAt(p, off)
	s.totalBytes += int64(written)
	return written, err
}

func (s *Stats) Close() {
	s.wrapped.Close()
}

func (s *Stats) PrintStats() {
	totalMb := s.totalBytes / 1024 / 1024
	duration := time.Since(s.staredAt)
	rate := float64(totalMb) / duration.Seconds()
	log.Infof("rate (MB/s): %.2f\n", rate)
}

func ExtractFromS3(
	ctx context.Context, awsClient *aws.Client, bucket, object, baseDir string, bufferSizeMiBs int, partMiBs int64, concurrency int,
) error {
	b := newRingBuffer(bufferSizeMiBs * 1024 * 1024) // 1GB

	stats := Stats{
		staredAt:   time.Now(),
		totalBytes: 0,
		wrapped:    b,
	}
	downloadRes := make(chan error, 1)
	stopStats := make(chan bool, 1)
	go func() {
		log.Info("Starting download")
		err := awsClient.DownloadFile(ctx, bucket, object, &stats, partMiBs, concurrency)
		stats.Close()
		stopStats <- true
		log.Infof("Done download, %v", err)
		downloadRes <- err
	}()
	go func() {
		t := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-t.C:
				stats.PrintStats()
			case <-stopStats:
				return
			}
		}
	}()

	d, err := zstd.NewReader(b)
	if err != nil {
		log.Errorf("Error creating zstd reader: %v", err)
		return err
	}
	defer d.Close()

	tr := tar.NewReader(d)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			log.Errorf("Error getting next tar entry: %v", err)
			return err
		}

		file := filepath.Join(baseDir, hdr.Name)
		if hdr.FileInfo().IsDir() {
			if err := os.MkdirAll(file, hdr.FileInfo().Mode()); err != nil {
				return err
			}
		} else {
			f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, hdr.FileInfo().Mode())
			if err != nil {
				return err
			}
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {
					log.Errorf("Error while closing file: %v", err)
				}
			}(f)

			fmt.Printf("Extracting %s\n", hdr.Name)
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}
		}
	}

	res := <-downloadRes
	return res
}
