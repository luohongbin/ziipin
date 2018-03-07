package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	Testdir       = "testdir"
	Truth         = "罗宏彬真的很帅很帅，哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈"
	SetSize int64 = 1024
	Prefix        = "prefix_"
	Suffix        = "_suffix"
	Name          = "luohongbin"
)

func base(t *testing.T) *FileWriteAgent {
	if err := os.RemoveAll(Testdir); err != nil {
		t.Fatal(err)
	}
	a := NewFileWriteAgent(Testdir)
	a.SetMaxSize(SetSize)
	return a
}

func TestSetMaxsize(t *testing.T) {
	a := base(t)
	for i := 0; i < 1000; i++ {
		a.WriteString(fmt.Sprintf("%s, %d\n", Truth, i))
	}
	err := filepath.Walk(Testdir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Size() > SetSize+int64(len(Truth)) {
			return fmt.Errorf("TestSetMaxsize fail:set size=%d,file size = %d", SetSize, info.Size())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetPrefix(t *testing.T) {
	a := base(t)
	a.SetPrefix(Prefix)
	for i := 0; i < 1000; i++ {
		a.WriteString(fmt.Sprintf("%s, %d\n", Truth, i))
	}
	err := filepath.Walk(Testdir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasPrefix(info.Name(), Prefix) {
			return fmt.Errorf("TestSetPrefix fail: %s", info.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetSuffix(t *testing.T) {
	a := base(t)
	a.SetSuffix(Suffix)
	for i := 0; i < 1000; i++ {
		a.WriteString(fmt.Sprintf("%s, %d\n", Truth, i))
	}
	err := filepath.Walk(Testdir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), Suffix) {
			return fmt.Errorf("TestSetSuffix fail: %s", info.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetAgentName(t *testing.T) {
	a := base(t)
	a.SetAgentName(Name)
	for i := 0; i < 1000; i++ {
		a.WriteString(fmt.Sprintf("%s, %d\n", Truth, i))
	}
	err := filepath.Walk(Testdir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.Contains(info.Name(), Name) {
			return fmt.Errorf("TestSetAgentName fail: %s", info.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClean(t *testing.T) {
	if err := os.RemoveAll(Testdir); err != nil {
		t.Fatal(err)
	}
}
