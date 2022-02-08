package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/dsa0x/sprout"
)

var (
	pathFlag string
	errFlag  string
	capFlag  int
)

var writer io.Writer = os.Stderr

var errHelp = `
# Usage:
#   sprout new [flags]
#	-path <path>
#		Path to the filter
#	-err_rate <float>
#		The desired false positive rate
#	-capacity <int>
#		The number of items intended to be added to the bloom filter (n)


#  sprout set [flags] element
#	-path <path>
#		Path to the filter


#  sprout get [flags] element
#	-path <path>
#		Path to the filter

#  sprout reset [flags]
#	-path <path>
#		Path to the filter


`

func init() {
	flag.StringVar(&pathFlag, "path", "", "Path to the filter")

	flag.ErrHelp = errors.New(errHelp)
	flag.Usage = func() {
		fmt.Fprint(writer, errHelp)
	}
}

func Execute() {
	flag.Parse()
	if len(os.Args) < 1 {
		flag.Usage()
		os.Exit(1)
	}
	command := os.Args[1]
	var element string
	flag.CommandLine.Parse(os.Args[2:])

	if command != "new" {
		if len(os.Args) <= 2 {
			flag.Usage()
			os.Exit(1)
		} else {
			element = os.Args[2]
		}
	}

	if command == "reset" && pathFlag == "" {
		flag.Usage()
		os.Exit(1)
	}

	if pathFlag == "" {
		pathFlag = "bloom.db"
	}

	bf := NewBloom()
	switch command {
	case "new":
		fmt.Fprintf(writer, "Filter %s created\n", pathFlag)
	case "set":
		bf.Add([]byte(element))
	case "get":
		resp := bf.Contains([]byte(element))
		fmt.Println(resp)
	case "reset":
		bf.Clear()
		fmt.Fprintf(writer, "Filter %s reset\n", pathFlag)
	case "stats":
		fmt.Printf("%+v\n", bf.Stats())
	default:
		flag.Usage()
	}
}

func NewBloom() *sprout.BloomFilter {
	opts := &sprout.BloomOptions{
		Path:     pathFlag,
		Capacity: 100,
		Err_rate: 0.001,
	}
	bf := sprout.NewBloom(opts)
	return bf
}
