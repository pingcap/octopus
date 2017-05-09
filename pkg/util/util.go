package util

import "fmt"

// Version information.
var (
	BuildTS = "None"
	GitHash = "None"
)

// PrintInfo prints the octopus version information
func PrintInfo() {
	fmt.Println("Git Commit Hash:", GitHash)
	fmt.Println("UTC Build Time: ", BuildTS)
}
