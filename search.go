package main

import (
	"path"
	"strconv"
	"os"
	"bufio"
	"io"
	"github.com/derekparker/trie"
	"sync"
)

/**
    Author: luzequan
    Created: 2018-08-29 10:56:42
*/

var trieCache = trie.New()

type StringSearch struct {
	trieMap sync.Map
	splitFn func(line []byte) (key, val string)
}

func NewStringSearch(splitFn func(line []byte) (key, val string)) *StringSearch {
	return &StringSearch{splitFn:splitFn}
}

func (s *StringSearch) Search(xid string) string {

	xidHash, _ := hashStr(xid)

	bucket := xidHash % bucketNum

	bucketList, ok := s.trieMap.Load(bucket)
	if !ok {
		var wg sync.WaitGroup
		wg.Add(1)
		go s.loadBucket(bucket, &wg)
		wg.Wait()

		bucketList, _ = s.trieMap.Load(bucket)
	}

	trieCache := bucketList.(*trie.Trie)

	node, ok := trieCache.Find(xid)
	if !ok {
		//fmt.Println("not found")
		return ""
	}
	//fmt.Println("xid[%s] found value [%v]", xid, node.Meta())
	return node.Meta().(string)
}

func (s *StringSearch) loadBucket(bucket uint32, wg *sync.WaitGroup) {

	t := trie.New()

	s.trieMap.Store(bucket, t)

	bucketStr := strconv.Itoa(int(bucket))

	bucketPath := path.Join(partitionPath, bucketStr)

	bucketFile, err := os.Open(bucketPath)
	defer bucketFile.Close()
	if err != nil {
		return
	}

	buf := bufio.NewReader(bucketFile)

	for {
		line, _, err := buf.ReadLine()

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}

		key, val := s.splitFn(line)

		//l := strings.Split(string(line), " ")

		t.Add(key, val)
	}

	wg.Done()
}

