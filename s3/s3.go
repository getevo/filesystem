package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/getevo/dsn"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type FileSystem struct {
	DSN           string `dsn:"s3://$AccessKey:$SecretKey@$Endpoint/$Bucket"`
	Scheme        string
	Region        string
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Bucket        string
	BasePath      string `default:""`
	Debug         bool   `default:"false"`
	IgnoreSSL     bool   `default:"false"`
	Params        map[string]string
	configuration aws.Config
	client        *s3.Client
}

func (l *FileSystem) DiskToStorage(src, dst string) error {
	const threshold = 50 * 1024 * 1024 // 50 MB

	fileInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("unable to stat local file: %w", err)
	}

	if fileInfo.Size() < threshold {
		return l.PutFile(src, dst)
	}
	return l.MultipartUploadFile(src, dst)
}

func (l *FileSystem) StorageToDisk(src, dst string) error {
	key := filepath.Join(l.BasePath, src)

	resp, err := l.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from s3: %w", err)
	}
	defer resp.Body.Close()

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Create or overwrite the file
	outFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer outFile.Close()

	// Copy contents from S3 to local file
	if _, err := io.Copy(outFile, resp.Body); err != nil {
		return fmt.Errorf("failed to copy data to local file: %w", err)
	}

	return nil
}

func (l *FileSystem) Setup(confString string) error {
	var err = dsn.ParseDSN(confString, l)
	var timeout = 60 * time.Second
	if v, ok := l.Params["Timeout"]; ok {
		timeout, err = time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("unable to parse s3 timeout parameter: %v", err)
		}
	}
	if l.Region == "" {
		l.Region = "us-east-1"
	}
	var httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: l.IgnoreSSL,
			},
		},
		Timeout: timeout,
	}
	l.configuration, err = config.LoadDefaultConfig(context.TODO(),
		config.WithHTTPClient(httpClient),
		config.WithRegion(l.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(l.AccessKey, l.SecretKey, "")),
	)

	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create S3 client with custom endpoint
	l.client = s3.NewFromConfig(l.configuration, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(l.Endpoint)
		o.UsePathStyle = true
	})

	_, err = l.client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(l.Bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to check bucket: %w", err)
	}
	return nil
}

func (l *FileSystem) Touch(path string) error {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}), // empty file
	})
	return err
}

func (l *FileSystem) Delete(path string) error {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func (l *FileSystem) List(path string) ([]string, error) {
	prefix := filepath.Join(l.BasePath, path)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var result []string
	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			result = append(result, strings.TrimPrefix(*obj.Key, prefix))
		}
	}
	return result, nil
}

func (l *FileSystem) Walk(path string, fn func(path string, info fs.FileInfo, err error) error) error {
	prefix := filepath.Join(l.BasePath, path)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return fn("", nil, err)
		}
		for _, obj := range page.Contents {
			fi := &FileInfo{
				key:  *obj.Key,
				size: *obj.Size,
				mod:  *obj.LastModified,
			}
			relPath := strings.TrimPrefix(*obj.Key, l.BasePath+"/")
			if err := fn(relPath, fi, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *FileSystem) Read(path string) ([]byte, error) {
	key := filepath.Join(l.BasePath, path)
	out, err := l.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (l *FileSystem) IsDir(path string) (bool, error) {
	prefix := filepath.Join(l.BasePath, path)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	out, err := l.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(l.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	return len(out.Contents) > 0, nil
}

func (l *FileSystem) IsFile(path string) (bool, error) {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *FileSystem) Mkdir(path string) error {
	key := filepath.Join(l.BasePath, path)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	return err
}

func (l *FileSystem) Write(path string, data []byte) error {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (l *FileSystem) WriteBuffer(path string, reader io.Reader) error {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	return err
}

func (l *FileSystem) Exists(path string) (bool, error) {
	key := filepath.Join(l.BasePath, path)
	_, err := l.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *FileSystem) Stat(path string) (fs.FileInfo, error) {
	key := filepath.Join(l.BasePath, path)
	head, err := l.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		key:  key,
		size: *head.ContentLength,
		mod:  *head.LastModified,
	}, nil
}

func (l *FileSystem) Copy(src, dst string) error {
	srcKey := filepath.Join(l.BasePath, src)
	dstKey := filepath.Join(l.BasePath, dst)

	source := l.Bucket + "/" + srcKey // required format: bucket/key

	_, err := l.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(l.Bucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(dstKey),
	})
	return err
}

func (l *FileSystem) Move(src, dst string) error {
	if err := l.Copy(src, dst); err != nil {
		return err
	}
	return l.Delete(src)
}

func (l *FileSystem) PutFile(localPath, s3Path string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	key := filepath.Join(l.BasePath, s3Path)
	_, err = l.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

func (l *FileSystem) MultipartUploadFile(localPath, s3Path string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	key := filepath.Join(l.BasePath, s3Path)

	// Initiate multipart upload
	createResp, err := l.client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	var (
		partSize       int64 = 5 * 1024 * 1024 // 5MB minimum
		numParts             = int((stat.Size() + partSize - 1) / partSize)
		completedParts       = make([]types.CompletedPart, 0, numParts)
	)

	for i := 0; i < numParts; i++ {
		partNum := int32(i + 1)
		start := int64(i) * partSize
		end := start + partSize
		if end > stat.Size() {
			end = stat.Size()
		}
		partLen := end - start

		partBuffer := make([]byte, partLen)
		if _, err := file.ReadAt(partBuffer, start); err != nil {
			return err
		}

		uploadResp, err := l.client.UploadPart(context.TODO(), &s3.UploadPartInput{
			Bucket:     aws.String(l.Bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNum),
			UploadId:   createResp.UploadId,
			Body:       bytes.NewReader(partBuffer),
		})
		if err != nil {
			// Abort on failure
			l.client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(l.Bucket),
				Key:      aws.String(key),
				UploadId: createResp.UploadId,
			})
			return fmt.Errorf("failed uploading part %d: %w", partNum, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNum),
		})
	}

	_, err = l.client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(l.Bucket),
		Key:      aws.String(key),
		UploadId: createResp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	return err
}

func New(configString string) (*FileSystem, error) {
	var s = &FileSystem{}
	if err := s.Setup(configString); err != nil {
		return s, err
	}
	return s, nil
}

type FileInfo struct {
	key  string
	size int64
	mod  time.Time
}

func (fi *FileInfo) Name() string       { return path.Base(fi.key) }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() fs.FileMode  { return 0444 }
func (fi *FileInfo) ModTime() time.Time { return fi.mod }
func (fi *FileInfo) IsDir() bool        { return strings.HasSuffix(fi.key, "/") }
func (fi *FileInfo) Sys() interface{}   { return nil }
