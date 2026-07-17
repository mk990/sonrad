//go:build !unix

package server

// diskspaceGB has no statfs on this platform; report a large value so the
// arrs' free-space check doesn't spuriously block downloads.
func diskspaceGB(string) (free, total float64) { return 1000, 1000 }
