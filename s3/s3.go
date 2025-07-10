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
	"github.com/getevo/filesystem"
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

	// Enhanced configuration
	options *filesystem.Options
	logger  filesystem.Logger
}

// defaultLogger provides a simple logger implementation for S3
type defaultLogger struct{}

func (d *defaultLogger) Debug(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fmt.Printf("[DEBUG] "+msg+"\n", fields...)
	} else {
		fmt.Printf("[DEBUG] %s\n", msg)
	}
}

func (d *defaultLogger) Info(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fmt.Printf("[INFO] "+msg+"\n", fields...)
	} else {
		fmt.Printf("[INFO] %s\n", msg)
	}
}

func (d *defaultLogger) Warn(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fmt.Printf("[WARN] "+msg+"\n", fields...)
	} else {
		fmt.Printf("[WARN] %s\n", msg)
	}
}

func (d *defaultLogger) Error(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fmt.Printf("[ERROR] "+msg+"\n", fields...)
	} else {
		fmt.Printf("[ERROR] %s\n", msg)
	}
}

// s3StreamReader implements filesystem.StreamReader for S3
type s3StreamReader struct {
	body io.ReadCloser
	size int64
}

func (sr *s3StreamReader) Read(p []byte) (n int, err error) {
	return sr.body.Read(p)
}

func (sr *s3StreamReader) Close() error {
	return sr.body.Close()
}

func (sr *s3StreamReader) Size() int64 {
	return sr.size
}

// s3StreamWriter implements filesystem.StreamWriter for S3
type s3StreamWriter struct {
	fs      *FileSystem
	key     string
	buffer  *bytes.Buffer
	ctx     context.Context
	aborted bool
}

func (sw *s3StreamWriter) Write(p []byte) (n int, err error) {
	if sw.aborted {
		return 0, errors.New("stream writer has been aborted")
	}
	return sw.buffer.Write(p)
}

func (sw *s3StreamWriter) Close() error {
	if sw.aborted {
		return errors.New("stream writer has been aborted")
	}

	_, err := sw.fs.client.PutObject(sw.ctx, &s3.PutObjectInput{
		Bucket: aws.String(sw.fs.Bucket),
		Key:    aws.String(sw.key),
		Body:   bytes.NewReader(sw.buffer.Bytes()),
	})
	return err
}

func (sw *s3StreamWriter) Abort() error {
	sw.aborted = true
	sw.buffer.Reset()
	return nil
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

// Enhanced methods with context support and new features

// SetupWithOptions configures the filesystem with options pattern
func (l *FileSystem) SetupWithOptions(opts *filesystem.Options) error {
	if opts == nil {
		opts = filesystem.DefaultOptions()
	}
	l.options = opts

	if opts.Logger != nil {
		l.logger = opts.Logger
	} else {
		l.logger = &defaultLogger{}
	}

	return nil
}

// Helper function to convert S3 errors to custom error types
func (l *FileSystem) handleS3Error(err error, path, op string) error {
	if err == nil {
		return nil
	}

	var nf *types.NotFound
	if errors.As(err, &nf) {
		return &filesystem.NotFoundError{Path: path, Op: op}
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return &filesystem.NotFoundError{Path: path, Op: op}
		case "AccessDenied", "Forbidden":
			return &filesystem.PermissionError{Path: path, Op: op}
		case "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
			return &filesystem.ExistsError{Path: path, Op: op}
		}
	}

	return err
}

// Context-aware methods
func (l *FileSystem) TouchWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("TouchWithContext: %s", key)
	}

	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}), // empty file
	})
	return l.handleS3Error(err, filePath, "touch")
}

func (l *FileSystem) DeleteWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("DeleteWithContext: %s", key)
	}

	_, err := l.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	return l.handleS3Error(err, filePath, "delete")
}

func (l *FileSystem) ListWithContext(ctx context.Context, filePath string) ([]filesystem.ListEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	prefix := path.Join(l.BasePath, filePath)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("ListWithContext: %s", prefix)
	}

	var result []filesystem.ListEntry
	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, l.handleS3Error(err, filePath, "list")
		}

		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix)
			if name == "" {
				continue
			}

			result = append(result, filesystem.ListEntry{
				Name:    name,
				IsDir:   strings.HasSuffix(*obj.Key, "/"),
				Size:    *obj.Size,
				ModTime: *obj.LastModified,
				Metadata: &filesystem.Metadata{
					ContentLength: *obj.Size,
					LastModified:  *obj.LastModified,
					ETag:          aws.ToString(obj.ETag),
					Custom:        make(map[string]string),
				},
			})
		}
	}
	return result, nil
}

func (l *FileSystem) WalkWithContext(ctx context.Context, filePath string, fn func(path string, info fs.FileInfo, err error) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	prefix := path.Join(l.BasePath, filePath)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("WalkWithContext: %s", prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fn("", nil, l.handleS3Error(err, filePath, "walk"))
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

func (l *FileSystem) ReadWithContext(ctx context.Context, filePath string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("ReadWithContext: %s", key)
	}

	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, l.handleS3Error(err, filePath, "read")
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	return data, err
}

func (l *FileSystem) IsDirWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	prefix := path.Join(l.BasePath, filePath)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsDirWithContext: %s", prefix)
	}

	out, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(l.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, l.handleS3Error(err, filePath, "isdir")
	}
	return len(out.Contents) > 0, nil
}

func (l *FileSystem) IsFileWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsFileWithContext: %s", key)
	}

	_, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, l.handleS3Error(err, filePath, "isfile")
	}
	return true, nil
}

func (l *FileSystem) MkdirWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("MkdirWithContext: %s", key)
	}

	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	return l.handleS3Error(err, filePath, "mkdir")
}

func (l *FileSystem) WriteWithContext(ctx context.Context, filePath string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteWithContext: %s", key)
	}

	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return l.handleS3Error(err, filePath, "write")
}

func (l *FileSystem) WriteBufferWithContext(ctx context.Context, filePath string, reader io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteBufferWithContext: %s", key)
	}

	_, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	return l.handleS3Error(err, filePath, "write")
}

func (l *FileSystem) ExistsWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("ExistsWithContext: %s", key)
	}

	_, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
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

func (l *FileSystem) StatWithContext(ctx context.Context, filePath string) (fs.FileInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	head, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, l.handleS3Error(err, filePath, "stat")
	}

	return &FileInfo{
		key:  key,
		size: *head.ContentLength,
		mod:  *head.LastModified,
	}, nil
}

func (l *FileSystem) CopyWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	srcKey := path.Join(l.BasePath, src)
	dstKey := path.Join(l.BasePath, dst)

	if l.options != nil && l.options.Debug {
		l.logger.Debug("CopyWithContext: %s -> %s", srcKey, dstKey)
	}

	source := l.Bucket + "/" + srcKey // required format: bucket/key

	_, err := l.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(l.Bucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(dstKey),
	})
	return l.handleS3Error(err, dst, "copy")
}

func (l *FileSystem) MoveWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := l.CopyWithContext(ctx, src, dst); err != nil {
		return err
	}
	return l.DeleteWithContext(ctx, src)
}

// Streaming methods
func (l *FileSystem) OpenReadStreamWithContext(ctx context.Context, filePath string) (filesystem.StreamReader, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenReadStreamWithContext: %s", key)
	}

	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, l.handleS3Error(err, filePath, "open_read_stream")
	}

	size := int64(0)
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}

	return &s3StreamReader{body: resp.Body, size: size}, nil
}

func (l *FileSystem) OpenWriteStreamWithContext(ctx context.Context, filePath string) (filesystem.StreamWriter, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenWriteStreamWithContext: %s", key)
	}

	return &s3StreamWriter{
		fs:     l,
		key:    key,
		buffer: &bytes.Buffer{},
		ctx:    ctx,
	}, nil
}

// Metadata methods
func (l *FileSystem) GetMetadataWithContext(ctx context.Context, filePath string) (*filesystem.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)
	head, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, l.handleS3Error(err, filePath, "get_metadata")
	}

	metadata := &filesystem.Metadata{
		ContentLength: *head.ContentLength,
		LastModified:  *head.LastModified,
		ETag:          aws.ToString(head.ETag),
		Custom:        make(map[string]string),
	}

	if head.ContentType != nil {
		metadata.ContentType = *head.ContentType
	}

	// Add custom metadata from S3 metadata
	for k, v := range head.Metadata {
		metadata.Custom[k] = v
	}

	return metadata, nil
}

func (l *FileSystem) SetMetadataWithContext(ctx context.Context, filePath string, metadata *filesystem.Metadata) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key := path.Join(l.BasePath, filePath)

	// For S3, we need to copy the object to itself with new metadata
	source := l.Bucket + "/" + key

	input := &s3.CopyObjectInput{
		Bucket:            aws.String(l.Bucket),
		CopySource:        aws.String(url.PathEscape(source)),
		Key:               aws.String(key),
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata:          make(map[string]string),
	}

	if metadata.ContentType != "" {
		input.ContentType = aws.String(metadata.ContentType)
	}

	// Add custom metadata
	for k, v := range metadata.Custom {
		input.Metadata[k] = v
	}

	_, err := l.client.CopyObject(ctx, input)
	return l.handleS3Error(err, filePath, "set_metadata")
}

// Transfer methods with context
func (l *FileSystem) DiskToStorageWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("DiskToStorageWithContext: %s -> %s", src, dst)
	}

	const threshold = 50 * 1024 * 1024 // 50 MB

	fileInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("unable to stat local file: %w", err)
	}

	if fileInfo.Size() < threshold {
		return l.putFileWithContext(ctx, src, dst)
	}
	return l.multipartUploadFileWithContext(ctx, src, dst)
}

func (l *FileSystem) StorageToDiskWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("StorageToDiskWithContext: %s -> %s", src, dst)
	}

	key := path.Join(l.BasePath, src)

	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return l.handleS3Error(err, src, "storage_to_disk")
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

// Helper methods for context-aware file operations
func (l *FileSystem) putFileWithContext(ctx context.Context, localPath, s3Path string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	key := path.Join(l.BasePath, s3Path)
	_, err = l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	return err
}

func (l *FileSystem) multipartUploadFileWithContext(ctx context.Context, localPath, s3Path string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	key := path.Join(l.BasePath, s3Path)

	// Initiate multipart upload
	createResp, err := l.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
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
		select {
		case <-ctx.Done():
			// Abort upload on context cancellation
			l.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(l.Bucket),
				Key:      aws.String(key),
				UploadId: createResp.UploadId,
			})
			return ctx.Err()
		default:
		}

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

		uploadResp, err := l.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(l.Bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNum),
			UploadId:   createResp.UploadId,
			Body:       bytes.NewReader(partBuffer),
		})
		if err != nil {
			// Abort on failure
			l.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
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

	_, err = l.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
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
	var s = &FileSystem{
		logger: &defaultLogger{},
	}
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
