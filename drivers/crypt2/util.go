package crypt2

import (
	stdpath "path"
	"path/filepath"
	"strings"

	rcCrypt "github.com/rclone/rclone/backend/crypt"
)

// will give the best guessing based on the path
func guessPath(path string) (isFolder, secondTry bool) {
	if strings.HasSuffix(path, "/") {
		//confirmed a folder
		return true, false
	}
	lastSlash := strings.LastIndex(path, "/")
	if !strings.Contains(path[lastSlash:], ".") {
		//no dot, try folder then try file
		return true, true
	}
	return false, true
}

func (d *Crypt) encryptPath(path string, isFolder bool) string {
	if isFolder {
		return d.cipher.EncryptDirName(path)
	}
	dir, fileName := filepath.Split(path)
	return stdpath.Join(d.cipher.EncryptDirName(dir), d.encFileName(fileName))
}

func (d *Crypt) encFileName(name string) string {
	return d.cipher.EncryptFileName(name) + d.EncryptedSuffix
}

func (d *Crypt) decFileName(name string) (string, error) {
	base, ok := strings.CutSuffix(name, d.EncryptedSuffix)
	if !ok || base == "" {
		return "", rcCrypt.ErrorNotAnEncryptedFile
	}
	return d.cipher.DecryptFileName(base)
}
