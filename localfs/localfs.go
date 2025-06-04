package localfs

import (
	"errors"
	"github.com/getevo/dsn"
	"github.com/getevo/evo/v2/lib/log"
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

func New(configString string) (*FileSystem, error) {
	var s = &FileSystem{}
	if err := s.Setup(configString); err != nil {
		return s, err
	}
	return s, nil
}
