package httpfs

import (
	"fmt"
	"github.com/getevo/dsn"
	"github.com/getevo/evo/v2/lib/curl"
	"github.com/getevo/evo/v2/lib/gpath"
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

func New(configString string) (*FileSystem, error) {
	var s = &FileSystem{}
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
