package main

import (
	"os"
	"bufio"
	"io"
	"strings"
	"sync"
	"fmt"
	"time"
	"path"
	"strconv"
	"path/filepath"
)
/**
    Author: luzequan
    Created: 2018-08-29 10:55:03
*/

const (
	primeRK = 16777619
	bucketNum = 10000
	sourceFilePath = "D:/GoglandProjects/src/stringsearch/test.xid"
	partitionPath = "D:/stringsearch/partitions1/"
)

var bucketMap sync.Map

type ResourceShard struct {
	splitFunc func(string)string
}

func (s *ResourceShard) Run() {

	startTime := time.Now()

	shardCh := make(chan string)

	for i:=0; i<100; i++ {
		go shard(shardCh, s.splitFunc)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go readSourceFile(shardCh, &wg)
	wg.Wait()

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Println("duration: ", duration)

	MkDirAll(partitionPath)

	bucketMap.Range(func(key, value interface{}) bool {
		tmpval, valid := value.([]string)
		if !valid {
			fmt.Println("invalid type assertion error", value)
			return true   //返回true，则range下一个key
		}

		bucketStr := strconv.Itoa(int(key.(uint32)))

		bucketPath := path.Join(partitionPath, bucketStr)

		var (
			outputfile *os.File
			err error
		)

		if IsFileExists(bucketPath) {
			outputfile, err = os.OpenFile(bucketPath, os.O_APPEND|os.O_WRONLY, 0644)
			defer outputfile.Close()
			if err != nil {
				return false
			}
		} else {
			outputfile, err = os.OpenFile(bucketPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
			defer outputfile.Close()
			if err != nil {

			}
		}

		outputfile.WriteString(strings.Join(tmpval, "\n"))

		return true
	})
}

func readSourceFile(shardCh chan string, wg *sync.WaitGroup) {
	sourceFile, err := os.Open(sourceFilePath)
	defer sourceFile.Close()
	if err != nil {
		return
	}

	buf := bufio.NewReader(sourceFile)

	for {
		line, _, err := buf.ReadLine()

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}

		shardCh <- string(line)
	}

	wg.Done()
}

func shard(shardCh chan string, splitFn func(string) string) {
	for str := range shardCh {

		xidHash, _ := hashStr(splitFn(str))

		bucket := xidHash % bucketNum

		l, ok := bucketMap.Load(bucket)
		if ok {
			list := l.([]string)
			list = append(list, str)
			bucketMap.Store(bucket, list)
		} else {
			list := make([]string, 0)
			list = append(list, str)
			bucketMap.Store(bucket, list)
		}
	}
}

func MkDirAll(Path string) {
	p := filepath.Clean(Path)
	d, err := os.Stat(p)
	if err != nil || !d.IsDir() {
		if err = os.MkdirAll(p, 0777); err != nil {
			//logs.Log.Error("创建路径失败[%v]: %v\n", Path, err)
		}
	}
}

func IsFileExists(path string) bool {
	fi, err := os.Stat(path)

	if err != nil {
		return os.IsExist(err)
	} else {
		return !fi.IsDir()
	}

	panic("util isFileExists not reached")
}


// hashStr returns the hash and the appropriate multiplicative
// factor for use in Rabin-Karp algorithm.
func hashStr(sep string) (uint32, uint32) {
	hash := uint32(0)
	for i := 0; i < len(sep); i++ {
		//hash = 0 * 16777619 + sep[i]
		hash = hash*primeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, primeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}
