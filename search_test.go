package main

import (
	"testing"
	"os"
	"bufio"
	"io"
	"strings"
	"fmt"
)

/**
    Author: luzequan
    Created: 2018-08-29 13:04:52
*/
func TestStringSearch(t *testing.T) {
	xid := "AAABAQAAAAXy6xrM2RxwqEFQrSaZPLRkwx51JTe0RuJmcTMKrcnViZwT0Lg="

	ss := NewStringSearch(splitFunc)

	ss.Search(xid)
}

func TestConcurrencyStringSearch(t *testing.T) {
	//xid := "AAABAQAAAAXy6xrM2RxwqEFQrSaZPLRkwx51JTe0RuJmcTMKrcnViZwT0Lg="
	//
	xidPath := "D:/GoglandProjects/src/stringsearch/test.target"

	ss := NewStringSearch(splitFunc)

	ch := make(chan string)

	for i:=0; i<50; i++ {
		go search(ss, ch)
	}

	testFile, err := os.Open(xidPath)
	defer testFile.Close()
	if err != nil {
		return
	}

	buf := bufio.NewReader(testFile)

	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}

		l := strings.Split(string(line), "|")
		xid := l[0]

		ch <- xid
	}
}

func search(ss *StringSearch, strCh chan string) {
	for xid := range strCh {
		reslt := ss.Search(xid)
		fmt.Println("result: ", reslt)
	}
}

func splitFunc(line []byte) (key, val string){
	l := strings.Split(string(line), " ")
	return l[1], l[0]
}
