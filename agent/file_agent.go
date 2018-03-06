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
		self.getNextFile()
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
		self.fInfo.f = nil
	}
}
func (self *FileWriteAgent) setcurrFileIndex() {
	var (
		maxModTime time.Time
		index      int
		err        error
		stat       os.FileInfo
	)
	for i := 0; i < MaxFileNum; i++ {
		fName := filepath.Join(self.dirPath,
			fmt.Sprintf(FileFormat, self.fInfo.prefix, self.agentName, i+1, self.fInfo.suffix))
		stat, err = os.Stat(fName)
		if err == nil {
			modTime := stat.ModTime()
			if modTime.After(maxModTime) {
				maxModTime = modTime
				index = i
			}
		}
	}
	self.fInfo.currFileIndex = index
}
func (self *FileWriteAgent) getNextFile() {
	var err error
	if self.fInfo.currFileIndex+1 >= MaxFileNum {
		self.fInfo.currFileIndex = 0
	} else {
		self.fInfo.currFileIndex += 1
	}
	if self.fInfo.f, err = os.OpenFile(self.getFPath(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755); err != nil {
		fmt.Println(err)
		return
	}
	if err = self.fInfo.f.Truncate(0); err != nil {
		fmt.Println(err)
		return
	}
	self.fInfo.fSize = 0
}
func (self *FileWriteAgent) setFInfo() {
	var (
		err error
	)
	if self.fInfo.f, err = os.OpenFile(self.getFPath(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755); err != nil {
		fmt.Println(err)
		return
	}
	if stat, err := self.fInfo.f.Stat(); err != nil {
		fmt.Println(err)
		return
	} else {
		self.fInfo.fSize = stat.Size()
	}
}
func (self *FileWriteAgent) getFPath() string {
	return filepath.Join(self.dirPath,
		fmt.Sprintf(FileFormat, self.fInfo.prefix, self.agentName,
			self.fInfo.currFileIndex+1, self.fInfo.suffix))
}
