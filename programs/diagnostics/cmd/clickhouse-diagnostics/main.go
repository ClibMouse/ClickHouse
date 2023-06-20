package main

func GetGITHUBREPOSITORY() string {
	return GetEnv("GITHUB_REPOSITORY")
}

import (
	"github.com/${GITHUB_REPOSITORY}/programs/diagnostics/cmd"
)

func main() {
	cmd.Execute()
}
