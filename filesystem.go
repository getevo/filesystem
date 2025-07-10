package filesystem

import (
	"context"
	"io"
	"io/fs"
	"time"
)

// Custom error types for common filesystem operations
type NotFoundError struct {
	Path string
	Op   string
}

func (e *NotFoundError) Error() string {
	return e.Op + " " + e.Path + ": no such file or directory"
}

type PermissionError struct {
	Path string
	Op   string
}

func (e *PermissionError) Error() string {
	return e.Op + " " + e.Path + ": permission denied"
}

type ExistsError struct {
	Path string
	Op   string
}

func (e *ExistsError) Error() string {
	return e.Op + " " + e.Path + ": file already exists"
}

type InvalidPathError struct {
	Path string
	Op   string
}

func (e *InvalidPathError) Error() string {
	return e.Op + " " + e.Path + ": invalid path"
}

// Logger interface for standardized logging
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

// Metadata represents file metadata and custom properties
type Metadata struct {
	ContentType   string
	ContentLength int64
	LastModified  time.Time
	ETag          string
	Custom        map[string]string
}

// Options pattern for configuration
type Options struct {
	// Connection settings
	Timeout       time.Duration
	RetryAttempts int
	RetryDelay    time.Duration

	// Logging
	Logger Logger
	Debug  bool

	// Security
	ReadOnly bool

	// Performance
	BufferSize     int
	ConnectionPool int
	EnableCaching  bool
	CacheTTL       time.Duration

	// Custom properties
	Custom map[string]interface{}
}

// DefaultOptions returns default configuration options
func DefaultOptions() *Options {
	return &Options{
		Timeout:        30 * time.Second,
		RetryAttempts:  3,
		RetryDelay:     time.Second,
		Debug:          false,
		ReadOnly:       false,
		BufferSize:     32 * 1024, // 32KB
		ConnectionPool: 10,
		EnableCaching:  false,
		CacheTTL:       5 * time.Minute,
		Custom:         make(map[string]interface{}),
	}
}

// StreamReader interface for streaming large file reads
type StreamReader interface {
	io.ReadCloser
	Size() int64
}

// StreamWriter interface for streaming large file writes
type StreamWriter interface {
	io.WriteCloser
	Abort() error
}

// ListEntry represents a file or directory entry with metadata
type ListEntry struct {
	Name     string
	IsDir    bool
	Size     int64
	ModTime  time.Time
	Metadata *Metadata
}

// Interface represents the enhanced filesystem interface with context support
type Interface interface {
	// Setup Configuration methods
	Setup(config string) error
	SetupWithOptions(opts *Options) error

	// TouchWithContext Context-aware methods (new enhanced interface)
	TouchWithContext(ctx context.Context, path string) error
	DeleteWithContext(ctx context.Context, path string) error
	ListWithContext(ctx context.Context, path string) ([]ListEntry, error)
	WalkWithContext(ctx context.Context, path string, fn func(path string, info fs.FileInfo, err error) error) error
	ReadWithContext(ctx context.Context, path string) ([]byte, error)
	IsDirWithContext(ctx context.Context, path string) (bool, error)
	IsFileWithContext(ctx context.Context, path string) (bool, error)
	MkdirWithContext(ctx context.Context, path string) error
	WriteWithContext(ctx context.Context, path string, data []byte) error
	WriteBufferWithContext(ctx context.Context, path string, writer io.Reader) error
	ExistsWithContext(ctx context.Context, path string) (bool, error)
	StatWithContext(ctx context.Context, path string) (fs.FileInfo, error)
	CopyWithContext(ctx context.Context, src, dst string) error
	MoveWithContext(ctx context.Context, src, dst string) error

	// OpenReadStreamWithContext Streaming methods
	OpenReadStreamWithContext(ctx context.Context, path string) (StreamReader, error)
	OpenWriteStreamWithContext(ctx context.Context, path string) (StreamWriter, error)

	// GetMetadataWithContext Metadata methods
	GetMetadataWithContext(ctx context.Context, path string) (*Metadata, error)
	SetMetadataWithContext(ctx context.Context, path string, metadata *Metadata) error

	// DiskToStorageWithContext Transfer methods with context
	DiskToStorageWithContext(ctx context.Context, src, dst string) error
	StorageToDiskWithContext(ctx context.Context, src, dst string) error

	// Touch Backward compatibility methods (existing interface)
	Touch(path string) error
	Delete(path string) error
	List(path string) ([]string, error)
	Walk(path string, fn func(path string, info fs.FileInfo, err error) error) error
	Read(path string) ([]byte, error)
	IsDir(path string) (bool, error)
	IsFile(path string) (bool, error)
	Mkdir(path string) error
	Write(path string, data []byte) error
	WriteBuffer(path string, writer io.Reader) error
	Exists(path string) (bool, error)
	Stat(path string) (fs.FileInfo, error)
	Copy(src, dst string) error
	Move(src, dst string) error
	DiskToStorage(src, dst string) error
	StorageToDisk(src, dst string) error
}
