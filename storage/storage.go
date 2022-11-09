package storage

import (
	"cs425mp3/utils"
	"io/ioutil"
)

type Manager interface {
	Store(file *File) error
}

var _ Manager = &Storage{}

type Storage struct {
	dir string
}

func New(dir string) Storage {
	return Storage{
		dir: dir,
	}
}

func (s Storage) Store(file *File) error {
	filedir := s.dir + file.name
	utils.CreateFileDirectory(filedir)
	if err := ioutil.WriteFile(filedir, file.buffer.Bytes(), 0644); err != nil {
		return err
	}

	return nil
}
