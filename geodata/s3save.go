//go:build migrations

package geodata

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config represents the location where the geodata DB is globally saved
type S3Config struct {
	Region string
	Bucket string
	Key    string
}

// SaveToS3 will upload the given SQLite DB to S3
func SaveToS3(ctx context.Context, cfg *S3Config, creds aws.CredentialsProvider, dbFile string) error {
	s3client := s3.New(s3.Options{
		Region:      cfg.Region,
		Credentials: creds,
	})

	db, err := os.Open(dbFile)
	if err != nil {
		return err
	}
	defer db.Close()

	mgr := manager.NewUploader(s3client)
	_, err = mgr.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(cfg.Key),
		Body:   db,
	})

	return err
}
