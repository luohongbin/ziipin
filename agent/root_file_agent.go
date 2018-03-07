package agent

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	MaxFileNum      = 5
	DefaultMaxFSize = 256 << 20
	DefaultPath     = "/tmp/file_agent"
	DefaultPrefix   = ""
	DefaultSuffix   = ".log"
)

var (
	fileAgent *FileWriteAgent
	agentName string = getExecName()
	dirPath   string = DefaultPath
	prefix    string = DefaultPrefix
	suffix    string = DefaultSuffix
	fMaxSize  int64  = DefaultMaxFSize
)

func GetFileAgent() *FileWriteAgent {
	if fileAgent == nil {
		fileAgent = NewFileWriteAgent(dirPath)
	}
	return fileAgent
}
func SetDirPath(dir string) {
	if err := checkAndBuildDir(dir); err != nil {
		panic(err.Error())
	}
	dirPath = dir
	a := GetFileAgent()
	a.SetDirPath(dir)
}
func SetAgentName(name string) {
	agentName = name
	a := GetFileAgent()
	a.SetAgentName(name)
}
func SetPrefix(p string) {
	prefix = p
	a := GetFileAgent()
	a.SetPrefix(prefix)
}
func SetSuffix(s string) {
	suffix = s
	a := GetFileAgent()
	a.SetSuffix(suffix)
}
func SetMaxSize(size int64) {
	fMaxSize = size
	a := GetFileAgent()
	a.SetMaxSize(size)
}

func Write(dataBuf []byte) {
	a := GetFileAgent()
	a.Write(dataBuf)
}
func WriteString(data string) {
	a := GetFileAgent()
	a.WriteString(data)
}

func checkAndBuildDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return nil
}

func getExecName() string {
	mainName := os.Args[0]
	index := strings.LastIndex(mainName, "/")
	if index+1 >= len(mainName) {
		panic("get main name fail: " + mainName)
	}
	return mainName[index+1:]
}

func NewFileWriteAgent(path string) *FileWriteAgent {
	if err := checkAndBuildDir(path); err != nil {
		panic(err.Error())
	}

	fileAgent := &FileWriteAgent{
		dirPath:   path,
		agentName: agentName,
		fInfo: FInfo{
			prefix:   prefix,
			suffix:   suffix,
			fMaxSize: fMaxSize,
		},
	}
	fileAgent.reset()
	return fileAgent
}

func IsNameOk(name string) bool {
	if name == "" {
		log.Println("param is not allowed empty")
		return false
	}
	ok := strings.Contains(name, "/")
	if ok {
		fmt.Println("name is not allowed to contain '/': ", name)
	}
	return !ok
}
