package main

import (
	"os"

	"github.com/pjtatlow/scurry/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
