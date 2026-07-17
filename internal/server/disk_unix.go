//go:build unix

package server

import "syscall"

// diskspaceGB returns free/total gigabytes of the filesystem holding path, so
// the SABnzbd API can report real disk space and Sonarr/Radarr's free-space
// checks actually work.
func diskspaceGB(path string) (free, total float64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	bs := float64(st.Bsize)
	return float64(st.Bavail) * bs / (1 << 30), float64(st.Blocks) * bs / (1 << 30)
}
