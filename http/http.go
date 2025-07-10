package httpfs

import (
	"context"
	"fmt"
	"github.com/getevo/dsn"
	"github.com/getevo/evo/v2/lib/curl"
	"github.com/getevo/evo/v2/lib/gpath"
	"github.com/getevo/filesystem"
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

type FileSystem struct {
	DSN    string `dsn:"https://$Host/$Path"`
	Scheme string
	Host   string
	Path   string
	Debug  bool `default:"false"`
	Params map[string]string

	headers curl.Header
	query   curl.QueryParam

	// Enhanced configuration
	options *filesystem.Options
	logger  filesystem.Logger
}

// defaultLogger provides a simple logger implementation for HTTP
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

// httpStreamReader implements filesystem.StreamReader for HTTP
type httpStreamReader struct {
	reader io.ReadCloser
	size   int64
}

func (hr *httpStreamReader) Read(p []byte) (n int, err error) {
	return hr.reader.Read(p)
}

func (hr *httpStreamReader) Close() error {
	return hr.reader.Close()
}

func (hr *httpStreamReader) Size() int64 {
	return hr.size
}

// httpStreamWriter implements filesystem.StreamWriter for HTTP (limited functionality)
type httpStreamWriter struct {
	fs      *FileSystem
	path    string
	buffer  []byte
	aborted bool
}

func (hw *httpStreamWriter) Write(p []byte) (n int, err error) {
	if hw.aborted {
		return 0, fmt.Errorf("stream writer has been aborted")
	}
	hw.buffer = append(hw.buffer, p...)
	return len(p), nil
}

func (hw *httpStreamWriter) Close() error {
	if hw.aborted {
		return fmt.Errorf("stream writer has been aborted")
	}
	// For HTTP, we would need to implement PUT/POST functionality
	return fmt.Errorf("HTTP write operations not implemented")
}

func (hw *httpStreamWriter) Abort() error {
	hw.aborted = true
	hw.buffer = nil
	return nil
}

type Params struct {
	Type string
	Name string
}

func (l *FileSystem) DiskToStorage(src, dst string) error {
	return fmt.Errorf("not implemented")
}

func (l *FileSystem) StorageToDisk(src, dst string) error {

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, src)
	if err != nil {
		return err
	}
	if l.Debug {
		fmt.Println("get file: " + result)
	}
	get, err := curl.Get(result, l.headers, l.query)
	if err != nil {
		return err
	}
	if get.Response().StatusCode != 200 {
		return fmt.Errorf("failed to get file, status code: %d", get.Response().StatusCode)
	}
	_ = gpath.MakePath(filepath.Dir(dst))
	err = get.ToFile(dst)
	return err
}

func (l *FileSystem) Setup(config string) error {
	var err = dsn.ParseDSN(config, l)
	l.Path = "/" + strings.Trim(l.Path, "/")
	l.headers = curl.Header{}
	l.query = curl.QueryParam{}

	for k, v := range l.Params {
		input, err := parseInput(k)
		if err == nil {
			if input.Type == "header" {
				l.headers[input.Name] = v
			} else if input.Type == "query" {
				l.query[input.Name] = v
			}
		}
	}

	return err
}

func (l *FileSystem) Touch(path string) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) Delete(path string) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) List(path string) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (l *FileSystem) Walk(path string, fn func(path string, info fs.FileInfo, err error) error) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) Read(path string) ([]byte, error) {

	return nil, fmt.Errorf("not implemented")
}

func (l *FileSystem) IsDir(path string) (bool, error) {

	return false, fmt.Errorf("not implemented")
}

func (l *FileSystem) IsFile(path string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (l *FileSystem) Mkdir(path string) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) Write(path string, data []byte) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) WriteBuffer(path string, r io.Reader) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) Exists(path string) (bool, error) {

	return false, fmt.Errorf("not implemented")
}

func (l *FileSystem) Stat(path string) (fs.FileInfo, error) {

	return nil, fmt.Errorf("not implemented")
}

func (l *FileSystem) Copy(src, dst string) error {

	return fmt.Errorf("not implemented")
}

func (l *FileSystem) Move(src, dst string) error {

	return fmt.Errorf("not implemented")
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

// Context-aware methods
func (l *FileSystem) TouchWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("TouchWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP touch operation not implemented")
}

func (l *FileSystem) DeleteWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("DeleteWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP delete operation not implemented")
}

func (l *FileSystem) ListWithContext(ctx context.Context, filePath string) ([]filesystem.ListEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("ListWithContext: %s", filePath)
	}

	return nil, fmt.Errorf("HTTP list operation not implemented")
}

func (l *FileSystem) WalkWithContext(ctx context.Context, filePath string, fn func(path string, info fs.FileInfo, err error) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("WalkWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP walk operation not implemented")
}

func (l *FileSystem) ReadWithContext(ctx context.Context, filePath string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("ReadWithContext: %s", filePath)
	}

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, filePath)
	if err != nil {
		return nil, &filesystem.InvalidPathError{Path: filePath, Op: "read"}
	}

	get, err := curl.Get(result, l.headers, l.query)
	if err != nil {
		return nil, err
	}

	if get.Response().StatusCode == 404 {
		return nil, &filesystem.NotFoundError{Path: filePath, Op: "read"}
	}

	if get.Response().StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error %d", get.Response().StatusCode)
	}

	return get.Bytes(), nil
}

func (l *FileSystem) IsDirWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsDirWithContext: %s", filePath)
	}

	return false, fmt.Errorf("HTTP directory check not implemented")
}

func (l *FileSystem) IsFileWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsFileWithContext: %s", filePath)
	}

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, filePath)
	if err != nil {
		return false, &filesystem.InvalidPathError{Path: filePath, Op: "isfile"}
	}

	// Use HEAD request to check if file exists
	head, err := curl.Head(result, l.headers, l.query)
	if err != nil {
		return false, err
	}

	return head.Response().StatusCode == 200, nil
}

func (l *FileSystem) MkdirWithContext(ctx context.Context, filePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("MkdirWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP mkdir operation not implemented")
}

func (l *FileSystem) WriteWithContext(ctx context.Context, filePath string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP write operation not implemented")
}

func (l *FileSystem) WriteBufferWithContext(ctx context.Context, filePath string, reader io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteBufferWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP write buffer operation not implemented")
}

func (l *FileSystem) ExistsWithContext(ctx context.Context, filePath string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("ExistsWithContext: %s", filePath)
	}

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, filePath)
	if err != nil {
		return false, &filesystem.InvalidPathError{Path: filePath, Op: "exists"}
	}

	// Use HEAD request to check if file exists
	head, err := curl.Head(result, l.headers, l.query)
	if err != nil {
		return false, err
	}

	return head.Response().StatusCode == 200, nil
}

func (l *FileSystem) StatWithContext(ctx context.Context, filePath string) (fs.FileInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("StatWithContext: %s", filePath)
	}

	return nil, fmt.Errorf("HTTP stat operation not implemented")
}

func (l *FileSystem) CopyWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("CopyWithContext: %s -> %s", src, dst)
	}

	return fmt.Errorf("HTTP copy operation not implemented")
}

func (l *FileSystem) MoveWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("MoveWithContext: %s -> %s", src, dst)
	}

	return fmt.Errorf("HTTP move operation not implemented")
}

// Streaming methods
func (l *FileSystem) OpenReadStreamWithContext(ctx context.Context, filePath string) (filesystem.StreamReader, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenReadStreamWithContext: %s", filePath)
	}

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, filePath)
	if err != nil {
		return nil, &filesystem.InvalidPathError{Path: filePath, Op: "open_read_stream"}
	}

	get, err := curl.Get(result, l.headers, l.query)
	if err != nil {
		return nil, err
	}

	if get.Response().StatusCode == 404 {
		return nil, &filesystem.NotFoundError{Path: filePath, Op: "open_read_stream"}
	}

	if get.Response().StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error %d", get.Response().StatusCode)
	}

	size := int64(0)
	if contentLength := get.Response().Header.Get("Content-Length"); contentLength != "" {
		// Parse content length if available
		// For simplicity, we'll leave it as 0
	}

	return &httpStreamReader{
		reader: get.Response().Body,
		size:   size,
	}, nil
}

func (l *FileSystem) OpenWriteStreamWithContext(ctx context.Context, filePath string) (filesystem.StreamWriter, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenWriteStreamWithContext: %s", filePath)
	}

	return &httpStreamWriter{
		fs:   l,
		path: filePath,
	}, nil
}

// Metadata methods
func (l *FileSystem) GetMetadataWithContext(ctx context.Context, filePath string) (*filesystem.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("GetMetadataWithContext: %s", filePath)
	}

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, filePath)
	if err != nil {
		return nil, &filesystem.InvalidPathError{Path: filePath, Op: "get_metadata"}
	}

	head, err := curl.Head(result, l.headers, l.query)
	if err != nil {
		return nil, err
	}

	if head.Response().StatusCode == 404 {
		return nil, &filesystem.NotFoundError{Path: filePath, Op: "get_metadata"}
	}

	if head.Response().StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error %d", head.Response().StatusCode)
	}

	metadata := &filesystem.Metadata{
		Custom: make(map[string]string),
	}

	if contentType := head.Response().Header.Get("Content-Type"); contentType != "" {
		metadata.ContentType = contentType
	}

	if contentLength := head.Response().Header.Get("Content-Length"); contentLength != "" {
		// Parse content length if needed
	}

	return metadata, nil
}

func (l *FileSystem) SetMetadataWithContext(ctx context.Context, filePath string, metadata *filesystem.Metadata) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.options != nil && l.options.Debug {
		l.logger.Debug("SetMetadataWithContext: %s", filePath)
	}

	return fmt.Errorf("HTTP set metadata operation not implemented")
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

	return fmt.Errorf("HTTP disk to storage operation not implemented")
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

	result, err := url.JoinPath(l.Scheme+"://"+l.Host, l.Path, src)
	if err != nil {
		return &filesystem.InvalidPathError{Path: src, Op: "storage_to_disk"}
	}

	get, err := curl.Get(result, l.headers, l.query)
	if err != nil {
		return err
	}

	if get.Response().StatusCode == 404 {
		return &filesystem.NotFoundError{Path: src, Op: "storage_to_disk"}
	}

	if get.Response().StatusCode != 200 {
		return fmt.Errorf("HTTP error %d", get.Response().StatusCode)
	}

	_ = gpath.MakePath(filepath.Dir(dst))
	err = get.ToFile(dst)
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

func parseInput(input string) (*Params, error) {
	// Regex to capture: type[name]
	re := regexp.MustCompile(`^([^\[\]=]+)\[([^\[\]=]+)\]$`)
	matches := re.FindStringSubmatch(input)

	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid input format")
	}

	return &Params{
		Type: strings.ToLower(matches[1]),
		Name: matches[2],
	}, nil
}
