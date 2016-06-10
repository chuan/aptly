package azure

import (
	"fmt"
	azure "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/smira/aptly/aptly"
	"github.com/smira/aptly/files"
	"io"
	"os"
	"path/filepath"
)

var maxBlockSizeInByte int = 4 * 1024 * 1024

// PublishedStorage abstract file system with published files (actually hosted on Azure)
type PublishedStorage struct {
	wasb      azure.BlobStorageClient
	container string
	prefix    string
	pathCache map[string]string
}

// Check interface
var (
	_ aptly.PublishedStorage = (*PublishedStorage)(nil)
)

func NewPublishedStorage(accountName, accountKey, container, prefix string) (*PublishedStorage, error) {
	client, err := azure.NewBasicClient(accountName, accountKey)
	result := &PublishedStorage{
		wasb:      client.GetBlobService(),
		container: container,
		prefix:    prefix,
	}
	return result, err
}

// String
func (storage *PublishedStorage) String() string {
	return fmt.Sprintf("Azure:%s/%s", storage.container, storage.prefix)
}

// MkDir creates directory recursively under public path
func (storage *PublishedStorage) MkDir(path string) error {
	// no op for Azure
	return nil
}

// PutFile puts file into published storage at specified path
func (storage *PublishedStorage) PutFile(path string, sourceFilename string) error {
	var (
		source *os.File
		err    error
	)
	source, err = os.Open(sourceFilename)
	if err != nil {
		return err
	}
	defer source.Close()

	path = filepath.Join(storage.prefix, path)
	err = storage.wasb.PutAppendBlob(storage.container, path, nil)
	if err != nil {
		return fmt.Errorf("error create blob for %s in %s: %s", sourceFilename, storage, err)
	}

	data := make([]byte, maxBlockSizeInByte)
	for {
		count, err := source.Read(data)
		if count == 0 && err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			err = storage.wasb.AppendBlock(storage.container, path, data, nil)
			if err != nil {
				return fmt.Errorf("error uploading %s to %s: %s", sourceFilename, storage, err)
			}
		}
	}

	return nil
}

// RemoveDirs removes directory structure under public path
func (storage *PublishedStorage) RemoveDirs(path string, progress aptly.Progress) error {
	filelist, err := storage.Filelist(path)
	if err != nil {
		return nil
	}

	for _, filename := range filelist {
		_, err := storage.wasb.DeleteBlobIfExists(storage.container, filepath.Join(storage.prefix, filename), nil)
		if err != nil {
			return fmt.Errorf("error deleting path %s from %s: %s", filename, storage, err)
		}
	}
	return nil
}

// Remove removes single file under public path
func (storage *PublishedStorage) Remove(path string) error {
	_, err := storage.wasb.DeleteBlobIfExists(storage.container, path, nil)
	if err != nil {
		return fmt.Errorf("error deleting %s from %s: %s", path, storage, err)
	}
	return nil
}

// LinkFromPool links package file from pool to dist's pool location
//
// publishedDirectory is desired location in pool (like prefix/pool/component/liba/libav/)
// sourcePool is instance of aptly.PackagePool
// sourcePath is filepath to package file in package pool
//
// LinkFromPool returns relative path for the published file to be included in package index
func (storage *PublishedStorage) LinkFromPool(publishedDirectory string, sourcePool aptly.PackagePool,
	sourcePath, sourceMD5 string, force bool) error {

	_ = sourcePool.(*files.PackagePool)

	baseName := filepath.Base(sourcePath)
	relPath := filepath.Join(publishedDirectory, baseName)
	poolPath := filepath.Join(storage.prefix, relPath)

	if storage.pathCache == nil {
		paths, md5s, err := storage.internalFilelist(relPath)
		if err != nil {
			return fmt.Errorf("error caching paths under prefix: %s", err)
		}

		storage.pathCache = make(map[string]string, len(paths))

		for i := range paths {
			storage.pathCache[paths[i]] = md5s[i]
		}
	}

	destinationMD5, exists := storage.pathCache[relPath]

	if exists {
		if destinationMD5 == sourceMD5 {
			return nil
		}

		if !force && destinationMD5 != sourceMD5 {
			return fmt.Errorf("error putting file to %s: file already exists and is different: %s", poolPath, storage)
		}
	}

	err := storage.PutFile(relPath, sourcePath)
	if err == nil {
		storage.pathCache[relPath] = sourceMD5
	}

	return err
}

func (storage *PublishedStorage) internalFilelist(prefix string) (paths []string, md5s []string, err error) {
	paths = make([]string, 0, 1024)
	md5s = make([]string, 0, 1024)
	prefix = filepath.Join(storage.prefix, prefix)
	if prefix != "" {
		prefix += "/"
	}

	marker := ""
	for {
		params := azure.ListBlobsParameters{
			Prefix:     prefix,
			MaxResults: 5000,
			Marker:     marker,
		}

		resp, err := storage.wasb.ListBlobs(storage.container, params)

		if err != nil {
			return nil, nil, fmt.Errorf("error listing under prefix %s in %s: %s", prefix, storage, err)
		} else {
			for _, blob := range resp.Blobs {
				paths = append(paths, blob.Name[len(prefix):])
				md5s = append(md5s, blob.Properties.ContentMD5)
			}

			if len(resp.NextMarker) > 0 {
				marker = resp.NextMarker
			} else {
				break
			}
		}
	}

	return paths, md5s, nil
}

// Filelist returns list of files under prefix
func (storage *PublishedStorage) Filelist(prefix string) ([]string, error) {
	paths, _, err := storage.internalFilelist(prefix)
	return paths, err
}

// RenameFile renames (moves) file
func (storage *PublishedStorage) RenameFile(oldName, newName string) error {
	sourceBlobUrl := storage.wasb.GetBlobURL(storage.container, filepath.Join(storage.prefix, oldName))
	err := storage.wasb.CopyBlob(storage.container, filepath.Join(storage.prefix, newName), sourceBlobUrl)
	if err != nil {
		return fmt.Errorf("error copying %s -> %s in %s: %s", oldName, newName, storage, err)
	}
	return storage.Remove(oldName)
}
