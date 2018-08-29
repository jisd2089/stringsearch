package main

import (
	"testing"
	"strings"
	"os"
)

/**
    Author: luzequan
    Created: 2018-08-29 15:06:16
*/
func initShard() {
	os.RemoveAll(partitionPath)
}

func TestShard(t *testing.T) {
	initShard()

	resourceShard := &ResourceShard{
		splitFunc: splitFn,
	}

	resourceShard.Run()
}

func splitFn(line string) string {
	return strings.Split(line, " ")[1]
}
