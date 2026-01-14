//go:build migrations

package geodata

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
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

type anonymousHTTPError interface {
	error
	HTTPStatusCode() int
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

	etagFile := fmt.Sprintf("%s.etag", dbFile)
	localEtag, err := getLocalETag(etagFile)
	if err != nil {
		return err
	}

	remoteEtag, err := getRemoteETag(ctx, s3client, cfg)
	if err != nil {
		return err
	}

	if localEtag == nil || remoteEtag == nil || *localEtag != *remoteEtag {
		mgr := manager.NewUploader(s3client)
		if _, err = mgr.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(cfg.Key),
			Body:   db,
		}); err != nil {
			return err
		}

		if remoteEtag == nil {
			remoteEtag, err = getRemoteETag(ctx, s3client, cfg)
			if err != nil {
				return err
			}
		}
	}

	if remoteEtag != nil {
		return os.WriteFile(etagFile, []byte(*remoteEtag), 0o666)
	}

	return nil
}

// CopyFromS3 will download the given SQLite DB from S3 for more migrations
func CopyFromS3(ctx context.Context, cfg *S3Config, creds aws.CredentialsProvider, dbFile string) error {
	s3client := s3.New(s3.Options{
		Region:      cfg.Region,
		Credentials: creds,
	})

	dbStat, err := os.Stat(dbFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	// if we use os.Create blindly, we'll overwrite an existing DB. If we use os.Open blindly, we'll get an
	// error if the DB is not there. We could use different flags to os.OpenFile but this is just as good
	var method = os.Create
	if dbStat != nil && !dbStat.IsDir() {
		method = os.Open
	}

	db, err := method(dbFile)
	if err != nil {
		return err
	}
	defer db.Close()

	etagFile := fmt.Sprintf("%s.etag", dbFile)

	localEtag, err := getLocalETag(etagFile)
	if err != nil {
		return err
	}

	remoteEtag, err := getRemoteETag(ctx, s3client, cfg)
	if err != nil {
		return err
	}

	if remoteEtag != nil {
		if err = os.WriteFile(etagFile, []byte(*remoteEtag), 0o666); err != nil {
			return err
		}
	}

	mgr := manager.NewDownloader(s3client)
	_, err = mgr.Download(ctx, db, &s3.GetObjectInput{
		Bucket:      aws.String(cfg.Bucket),
		Key:         aws.String(cfg.Key),
		IfNoneMatch: localEtag,
	})

	if err != nil {
		var ae anonymousHTTPError
		if errors.As(err, &ae) {
			if ae.HTTPStatusCode() == http.StatusNotModified || ae.HTTPStatusCode() == http.StatusNotFound {
				return nil
			}
		}
	}

	return err
}

func getLocalETag(etagFile string) (*string, error) {
	etagBytes, err := os.ReadFile(etagFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		return nil, nil
	}

	localEtag := string(etagBytes)
	return &localEtag, nil
}

func getRemoteETag(ctx context.Context, s3client *s3.Client, cfg *S3Config) (*string, error) {
	out, err := s3client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(cfg.Key),
	})
	if err != nil {
		var ae anonymousHTTPError
		if errors.As(err, &ae) {
			if ae.HTTPStatusCode() == http.StatusNotFound {
				return nil, nil
			}
		}

		return nil, err
	}

	return out.ETag, nil
}
