package localfs

import (
	"context"
	"errors"
	"fmt"
	"github.com/getevo/dsn"
	"github.com/getevo/evo/v2/lib/log"
	"github.com/getevo/filesystem"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileSystem struct {
	DSN    string `dsn:"fs://$Path"`
	Scheme string
	Path   string
	Debug  bool `default:"false"`
	Params map[string]string

	// Enhanced configuration
	options *filesystem.Options
	logger  filesystem.Logger
}

// defaultLogger provides a simple logger implementation
type defaultLogger struct{}

func (d *defaultLogger) Debug(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		log.Debug(fmt.Sprintf(msg, fields...))
	} else {
		log.Debug(msg)
	}
}

func (d *defaultLogger) Info(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		log.Info(fmt.Sprintf(msg, fields...))
	} else {
		log.Info(msg)
	}
}

func (d *defaultLogger) Warn(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		log.Info(fmt.Sprintf(msg, fields...))
	} else {
		log.Info(msg)
	}
}

func (d *defaultLogger) Error(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		log.Error(fmt.Sprintf(msg, fields...))
	} else {
		log.Error(msg)
	}
}

// streamReader implements filesystem.StreamReader
type streamReader struct {
	*os.File
	size int64
}

func (sr *streamReader) Size() int64 {
	return sr.size
}

// streamWriter implements filesystem.StreamWriter
type streamWriter struct {
	*os.File
	path string
}

func (sw *streamWriter) Abort() error {
	sw.File.Close()
	return os.Remove(sw.path)
}

func (l *FileSystem) DiskToStorage(src, dst string) error {
	if l.Debug {
		log.Info("DiskToStorage: %s -> %s", src, dst)
	}
	return l.Copy(src, dst)
}

func (l *FileSystem) StorageToDisk(src, dst string) error {
	if l.Debug {
		log.Info("StorageToDisk: %s -> %s", src, dst)
	}
	return l.Copy(src, dst)
}

func (l *FileSystem) Setup(config string) error {
	var err = dsn.ParseDSN(config, l)
	l.Path = "/" + strings.Trim(l.Path, "/")
	return err
}

func (l *FileSystem) Touch(path string) error {
	fullPath := l.resolve(path)

	if l.Debug {
		log.Info("Touch: %s", fullPath)
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	// If file exists, update mod time
	if _, err := os.Stat(fullPath); err == nil {
		return os.Chtimes(fullPath, time.Now(), time.Now())
	}

	// If not, create an empty file
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	return file.Close()
}

func (l *FileSystem) Delete(path string) error {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Delete: %s", fullPath)
	}
	return os.RemoveAll(fullPath)
}

func (l *FileSystem) List(path string) ([]string, error) {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("List: %s", fullPath)
	}
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}

func (l *FileSystem) Walk(path string, fn func(path string, info fs.FileInfo, err error) error) error {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Walk: %s", fullPath)
	}
	return filepath.Walk(fullPath, func(p string, info os.FileInfo, err error) error {
		// We return the relative path from basePath to maintain logical consistency
		relPath, _ := filepath.Rel(l.Path, p)
		return fn(relPath, info, err)
	})
}

func (l *FileSystem) Read(path string) ([]byte, error) {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Read: %s", fullPath)
	}
	return os.ReadFile(fullPath)
}

func (l *FileSystem) IsDir(path string) (bool, error) {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("IsDir: %s", fullPath)
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func (l *FileSystem) IsFile(path string) (bool, error) {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("IsFile: %s", fullPath)
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return false, err
	}
	return !info.IsDir(), nil
}

func (l *FileSystem) Mkdir(path string) error {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Mkdir: %s", fullPath)
	}
	return os.MkdirAll(fullPath, 0755)
}

func (l *FileSystem) Write(path string, data []byte) error {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Write: %s", fullPath)
	}
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	// Check if file exists
	_, err := os.Stat(fullPath)
	if err == nil {
		// File exists, open with truncation
		file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = file.Write(data)
		return err
	}

	if os.IsNotExist(err) {
		// File does not exist, create and write
		return os.WriteFile(fullPath, data, 0644)
	}

	// Any other stat error
	return err
}

func (l *FileSystem) WriteBuffer(path string, r io.Reader) error {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("WriteBuffer: %s", fullPath)
	}
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	// Check if file exists
	_, err := os.Stat(fullPath)
	if err == nil {
		// File exists, open with truncation
		file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(file, r)
		return err
	}

	if os.IsNotExist(err) {
		// File does not exist, create it
		file, err := os.Create(fullPath)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(file, r)
		return err
	}

	// Any other stat error
	return err
}

func (l *FileSystem) Exists(path string) (bool, error) {
	fullPath := l.resolve(path)
	if l.Debug {
		log.Info("Exists: %s", fullPath)
	}
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (l *FileSystem) Stat(path string) (fs.FileInfo, error) {
	fullPath := l.resolve(path)
	return os.Stat(fullPath)
}

func (l *FileSystem) resolve(p string) string {
	joined := filepath.Join(l.Path, p)
	cleaned := filepath.Clean(joined)
	// Ensure the resolved path is still within basePath
	if !strings.HasPrefix(cleaned, l.Path) {
		// If someone tries to escape, fallback to basePath
		return l.Path
	}

	return cleaned
}

func (l *FileSystem) Copy(src, dst string) error {
	srcPath := l.resolve(src)
	if l.Debug {
		log.Info("Copy: %s -> %s", srcPath, dst)
	}
	if srcPath == dst {
		return errors.New("source and destination paths cannot be the same")
	}

	// Open source file
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Ensure destination directory exists
	if err = os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	// Create or truncate a destination file
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func (l *FileSystem) Move(src, dst string) error {
	srcPath := l.resolve(src)
	if l.Debug {
		log.Info("Move: %s -> %s", srcPath, dst)
	}
	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	// If destination file exists, remove it (optional but avoids Rename errors)
	if _, err := os.Stat(dst); err == nil {
		if err := os.Remove(dst); err != nil {
			return err
		}
	}

	// Attempt to move (rename) the file
	return os.Rename(srcPath, dst)
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
func (l *FileSystem) TouchWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("TouchWithContext: %s", fullPath)
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return &filesystem.PermissionError{Path: path, Op: "touch"}
	}

	// If file exists, update mod time
	if _, err := os.Stat(fullPath); err == nil {
		return os.Chtimes(fullPath, time.Now(), time.Now())
	}

	// If not, create an empty file
	file, err := os.Create(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &filesystem.NotFoundError{Path: path, Op: "touch"}
		}
		if os.IsPermission(err) {
			return &filesystem.PermissionError{Path: path, Op: "touch"}
		}
		return err
	}
	return file.Close()
}

func (l *FileSystem) DeleteWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("DeleteWithContext: %s", fullPath)
	}

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return &filesystem.NotFoundError{Path: path, Op: "delete"}
	}

	err := os.RemoveAll(fullPath)
	if err != nil && os.IsPermission(err) {
		return &filesystem.PermissionError{Path: path, Op: "delete"}
	}
	return err
}

func (l *FileSystem) ListWithContext(ctx context.Context, path string) ([]filesystem.ListEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("ListWithContext: %s", fullPath)
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &filesystem.NotFoundError{Path: path, Op: "list"}
		}
		if os.IsPermission(err) {
			return nil, &filesystem.PermissionError{Path: path, Op: "list"}
		}
		return nil, err
	}

	var result []filesystem.ListEntry
	for _, entry := range entries {
		info, _ := entry.Info()
		result = append(result, filesystem.ListEntry{
			Name:    entry.Name(),
			IsDir:   entry.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Metadata: &filesystem.Metadata{
				ContentLength: info.Size(),
				LastModified:  info.ModTime(),
				Custom:        make(map[string]string),
			},
		})
	}
	return result, nil
}

func (l *FileSystem) WalkWithContext(ctx context.Context, path string, fn func(path string, info fs.FileInfo, err error) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("WalkWithContext: %s", fullPath)
	}

	return filepath.Walk(fullPath, func(p string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Return the relative path from basePath to maintain logical consistency
		relPath, _ := filepath.Rel(l.Path, p)
		return fn(relPath, info, err)
	})
}

func (l *FileSystem) ReadWithContext(ctx context.Context, path string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("ReadWithContext: %s", fullPath)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &filesystem.NotFoundError{Path: path, Op: "read"}
		}
		if os.IsPermission(err) {
			return nil, &filesystem.PermissionError{Path: path, Op: "read"}
		}
	}
	return data, err
}

func (l *FileSystem) IsDirWithContext(ctx context.Context, path string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsDirWithContext: %s", fullPath)
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, &filesystem.NotFoundError{Path: path, Op: "isdir"}
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (l *FileSystem) IsFileWithContext(ctx context.Context, path string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("IsFileWithContext: %s", fullPath)
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, &filesystem.NotFoundError{Path: path, Op: "isfile"}
		}
		return false, err
	}
	return !info.IsDir(), nil
}

func (l *FileSystem) MkdirWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("MkdirWithContext: %s", fullPath)
	}

	err := os.MkdirAll(fullPath, 0755)
	if err != nil && os.IsPermission(err) {
		return &filesystem.PermissionError{Path: path, Op: "mkdir"}
	}
	return err
}

func (l *FileSystem) WriteWithContext(ctx context.Context, path string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteWithContext: %s", fullPath)
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return &filesystem.PermissionError{Path: path, Op: "write"}
	}

	err := os.WriteFile(fullPath, data, 0644)
	if err != nil && os.IsPermission(err) {
		return &filesystem.PermissionError{Path: path, Op: "write"}
	}
	return err
}

func (l *FileSystem) WriteBufferWithContext(ctx context.Context, path string, reader io.Reader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("WriteBufferWithContext: %s", fullPath)
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return &filesystem.PermissionError{Path: path, Op: "write"}
	}

	file, err := os.Create(fullPath)
	if err != nil {
		if os.IsPermission(err) {
			return &filesystem.PermissionError{Path: path, Op: "write"}
		}
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	return err
}

func (l *FileSystem) ExistsWithContext(ctx context.Context, path string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("ExistsWithContext: %s", fullPath)
	}

	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (l *FileSystem) StatWithContext(ctx context.Context, path string) (fs.FileInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	info, err := os.Stat(fullPath)
	if err != nil && os.IsNotExist(err) {
		return nil, &filesystem.NotFoundError{Path: path, Op: "stat"}
	}
	return info, err
}

func (l *FileSystem) CopyWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	srcPath := l.resolve(src)
	dstPath := l.resolve(dst)

	if l.options != nil && l.options.Debug {
		l.logger.Debug("CopyWithContext: %s -> %s", srcPath, dstPath)
	}

	if srcPath == dstPath {
		return &filesystem.InvalidPathError{Path: src, Op: "copy"}
	}

	// Open source file
	srcFile, err := os.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &filesystem.NotFoundError{Path: src, Op: "copy"}
		}
		if os.IsPermission(err) {
			return &filesystem.PermissionError{Path: src, Op: "copy"}
		}
		return err
	}
	defer srcFile.Close()

	// Ensure destination directory exists
	if err = os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		return &filesystem.PermissionError{Path: dst, Op: "copy"}
	}

	// Create destination file
	dstFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		if os.IsPermission(err) {
			return &filesystem.PermissionError{Path: dst, Op: "copy"}
		}
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func (l *FileSystem) MoveWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	srcPath := l.resolve(src)
	dstPath := l.resolve(dst)

	if l.options != nil && l.options.Debug {
		l.logger.Debug("MoveWithContext: %s -> %s", srcPath, dstPath)
	}

	// Check if source exists
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		return &filesystem.NotFoundError{Path: src, Op: "move"}
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		return &filesystem.PermissionError{Path: dst, Op: "move"}
	}

	// If destination file exists, remove it
	if _, err := os.Stat(dstPath); err == nil {
		if err := os.Remove(dstPath); err != nil {
			return &filesystem.PermissionError{Path: dst, Op: "move"}
		}
	}

	// Attempt to move (rename) the file
	err := os.Rename(srcPath, dstPath)
	if err != nil && os.IsPermission(err) {
		return &filesystem.PermissionError{Path: src, Op: "move"}
	}
	return err
}

// Streaming methods
func (l *FileSystem) OpenReadStreamWithContext(ctx context.Context, path string) (filesystem.StreamReader, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenReadStreamWithContext: %s", fullPath)
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &filesystem.NotFoundError{Path: path, Op: "open_read_stream"}
		}
		if os.IsPermission(err) {
			return nil, &filesystem.PermissionError{Path: path, Op: "open_read_stream"}
		}
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &streamReader{File: file, size: info.Size()}, nil
}

func (l *FileSystem) OpenWriteStreamWithContext(ctx context.Context, path string) (filesystem.StreamWriter, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	if l.options != nil && l.options.Debug {
		l.logger.Debug("OpenWriteStreamWithContext: %s", fullPath)
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, &filesystem.PermissionError{Path: path, Op: "open_write_stream"}
	}

	file, err := os.Create(fullPath)
	if err != nil {
		if os.IsPermission(err) {
			return nil, &filesystem.PermissionError{Path: path, Op: "open_write_stream"}
		}
		return nil, err
	}

	return &streamWriter{File: file, path: fullPath}, nil
}

// Metadata methods
func (l *FileSystem) GetMetadataWithContext(ctx context.Context, path string) (*filesystem.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fullPath := l.resolve(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &filesystem.NotFoundError{Path: path, Op: "get_metadata"}
		}
		return nil, err
	}

	return &filesystem.Metadata{
		ContentLength: info.Size(),
		LastModified:  info.ModTime(),
		Custom:        make(map[string]string),
	}, nil
}

func (l *FileSystem) SetMetadataWithContext(ctx context.Context, path string, metadata *filesystem.Metadata) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fullPath := l.resolve(path)

	// For local filesystem, we can only set modification time
	if !metadata.LastModified.IsZero() {
		err := os.Chtimes(fullPath, time.Now(), metadata.LastModified)
		if err != nil {
			if os.IsNotExist(err) {
				return &filesystem.NotFoundError{Path: path, Op: "set_metadata"}
			}
			if os.IsPermission(err) {
				return &filesystem.PermissionError{Path: path, Op: "set_metadata"}
			}
		}
		return err
	}

	return nil
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
	return l.CopyWithContext(ctx, src, dst)
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
	return l.CopyWithContext(ctx, src, dst)
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
