package filesystem

import (
	"io"
	"io/fs"
)

type Interface interface {
	Setup(config string) error
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
