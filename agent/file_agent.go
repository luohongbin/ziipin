package agent

import (
	"fmt"

	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	FileFormat = "%s%s_%d%s"
)

type FInfo struct {
	f             *os.File
	prefix        string
	suffix        string
	currFileIndex int
	fSize         int64
	fMaxSize      int64
}
type FileWriteAgent struct {
	m         sync.Mutex
	dirPath   string
	agentName string
	fInfo     FInfo
}

func (self *FileWriteAgent) SetAgentName(name string) *FileWriteAgent {
	if !IsNameOk(name) {
		return self
	}

	self.m.Lock()
	defer self.m.Unlock()

	if self.agentName != name {
		self.agentName = name
		self.reset()
	}
	return self
}
func (self *FileWriteAgent) SetPrefix(prefix string) *FileWriteAgent {
	if !IsNameOk(prefix) {
		return self
	}
	self.m.Lock()
	defer self.m.Unlock()

	if self.fInfo.prefix != prefix {
		self.fInfo.prefix = prefix
		self.reset()
	}
	return self
}
func (self *FileWriteAgent) SetSuffix(suffix string) *FileWriteAgent {
	if !IsNameOk(suffix) {
		return self
	}
	self.m.Lock()
	defer self.m.Unlock()

	if self.fInfo.suffix != suffix {
		self.fInfo.suffix = suffix
		self.reset()
	}
	return self
}
func (self *FileWriteAgent) SetDirPath(dirPath string) *FileWriteAgent {
	self.m.Lock()
	defer self.m.Unlock()

	if err := checkAndBuildDir(dirPath); err != nil {
		fmt.Println(err.Error())
		return self
	}

	if self.dirPath != dirPath {
		self.dirPath = dirPath
		self.reset()
	}
	return self
}
func (self *FileWriteAgent) SetMaxSize(size int64) *FileWriteAgent {
	self.m.Lock()
	defer self.m.Unlock()
	if size <= 0 {
		fmt.Println("size must be bigger than 0")
		return self
	}

	self.fInfo.fMaxSize = size
	return self
}

func (self *FileWriteAgent) Write(data string) {
	self.m.Lock()
	defer self.m.Unlock()

	if self.fInfo.f == nil {
		self.setFInfo()
	}
	eLength := len(data)
	if self.fInfo.fSize >= self.fInfo.fMaxSize {
		if self.fInfo.currFileIndex+1 >= MaxFileNum {
			self.fInfo.currFileIndex = 0
		} else {
			self.fInfo.currFileIndex += 1
		}

		self.setFInfo()
	}

	_, err := self.fInfo.f.WriteString(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	self.fInfo.fSize += int64(eLength)
}
func (self *FileWriteAgent) Close() {
	if self.fInfo.f != nil {
		self.fInfo.f.Close()
	}
}
func (self *FileWriteAgent) reset() {
	self.setcurrFileIndex()
	if self.fInfo.f != nil {
		self.fInfo.f.Close()
	}
}
func (self *FileWriteAgent) setcurrFileIndex() {
	var (
		minModTime time.Time = time.Now()
		index      int
		err        error
		stat       os.FileInfo
	)
	for i := 0; i < MaxFileNum; i++ {
		fName := filepath.Join(self.dirPath,
			fmt.Sprintf(FileFormat, self.fInfo.prefix, self.agentName, MaxFileNum-i, self.fInfo.suffix))
		stat, err = os.Stat(fName)
		if err == nil {
			modTime := stat.ModTime()
			if modTime.Before(minModTime) {
				minModTime = modTime
				index = i
			}
		} else if os.IsNotExist(err) {
			self.fInfo.currFileIndex = i
			return
		} else {
			panic(err.Error())
		}
	}
	self.fInfo.currFileIndex = index
}
func (self *FileWriteAgent) setFInfo() {
	var (
		err error
	)
	fPath := filepath.Join(self.dirPath,
		fmt.Sprintf(FileFormat, self.fInfo.prefix, self.agentName, self.fInfo.currFileIndex+1, self.fInfo.suffix))
	if self.fInfo.f, err = os.OpenFile(fPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755); err != nil {
		fmt.Println(err)
	}
	if err = self.fInfo.f.Truncate(0); err != nil {
		fmt.Println(err)
	}
	self.fInfo.fSize = 0
}
