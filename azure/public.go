package azure

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aptly-dev/aptly/aptly"
	"github.com/aptly-dev/aptly/files"
	"github.com/aptly-dev/aptly/utils"
	"github.com/pkg/errors"
)

// PublishedStorage abstract file system with published files (actually hosted on Azure)
type PublishedStorage struct {
	container azblob.ContainerURL
	prefix    string
	pathCache map[string]string
}

// Check interface
var (
	_ aptly.PublishedStorage = (*PublishedStorage)(nil)
)

// NewPublishedStorage creates published storage from Azure storage credentials
func NewPublishedStorage(accountName, accountKey, container, prefix string) (*PublishedStorage, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	containerUrl, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, container))
	if err != nil {
		return nil, err
	}

	result := &PublishedStorage{
		container: azblob.NewContainerURL(*containerUrl, azblob.NewPipeline(credential, azblob.PipelineOptions{})),
		prefix:    prefix,
	}

	return result, nil
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

	blob := storage.container.NewBlockBlobURL(path)

	uploadOptions := azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16}

	_, err = azblob.UploadFileToBlockBlob(
		context.Background(),
		source,
		blob,
		uploadOptions)

	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("error uploading %s to %s", sourceFilename, storage))
	}

	return err
}

// RemoveDirs removes directory structure under public path
func (storage *PublishedStorage) RemoveDirs(path string, progress aptly.Progress) error {
	filelist, err := storage.Filelist(path)
	if err != nil {
		return nil
	}

	for _, filename := range filelist {
		blob := storage.container.NewBlobURL(filepath.Join(storage.prefix, path, filename))
		_, err := blob.Delete(context.Background(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("error deleting path %s from %s: %s", filename, storage, err))
		}
	}
	return err
}

// Remove removes single file under public path
func (storage *PublishedStorage) Remove(path string) error {
	blob := storage.container.NewBlobURL(path)
	_, err := blob.Delete(context.Background(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("error deleting %s from %s: %s", path, storage, err))
	}
	return err
}

// LinkFromPool links package file from pool to dist's pool location
//
// publishedDirectory is desired location in pool (like prefix/pool/component/liba/libav/)
// sourcePool is instance of aptly.PackagePool
// sourcePath is filepath to package file in package pool
//
// LinkFromPool returns relative path for the published file to be included in package index
func (storage *PublishedStorage) LinkFromPool(publishedDirectory, fileName string, sourcePool aptly.PackagePool,
	sourcePath string, sourceChecksums utils.ChecksumInfo, force bool) error {

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
	sourceMD5 := sourceChecksums.MD5

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
	const delimiter = "/"
	paths = make([]string, 0, 1024)
	md5s = make([]string, 0, 1024)
	prefix = filepath.Join(storage.prefix, prefix)
	if prefix != "" {
		prefix += delimiter
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := storage.container.ListBlobsHierarchySegment(
			context.Background(), marker, delimiter, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("error listing under prefix %s in %s: %s", prefix, storage, err)
		}

		marker = listBlob.NextMarker

		for _, blob := range listBlob.Segment.BlobItems {
			if prefix == "" {
				paths = append(paths, blob.Name)
			} else {
				paths = append(paths, blob.Name[len(prefix):])
			}
			md5s = append(md5s, fmt.Sprintf("%x", blob.Properties.ContentMD5))
		}
	}

	return paths, md5s, nil
}

// Filelist returns list of files under prefix
func (storage *PublishedStorage) Filelist(prefix string) ([]string, error) {
	paths, _, err := storage.internalFilelist(prefix)
	return paths, err
}

// Internal copy or move implementation
func (storage *PublishedStorage) internalCopyOrMoveBlob(src, dst string, metadata azblob.Metadata, move bool) error {
	const leaseDuration = 30

	dstBlobUrl := storage.container.NewBlobURL(filepath.Join(storage.prefix, dst))
	leaseResp, err := dstBlobUrl.AcquireLease(context.Background(), "", leaseDuration, azblob.ModifiedAccessConditions{})
	if err != nil || leaseResp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("error acquiring lease on destination blob %s", dstBlobUrl)
	}
	defer dstBlobUrl.BreakLease(context.Background(), azblob.LeaseBreakNaturally, azblob.ModifiedAccessConditions{})

	dstBlobLeaseId := leaseResp.LeaseID()

	srcBlobUrl := storage.container.NewBlobURL(filepath.Join(storage.prefix, src))
	leaseResp, err = srcBlobUrl.AcquireLease(context.Background(), "", leaseDuration, azblob.ModifiedAccessConditions{})
	if err != nil || leaseResp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("error acquiring lease on source blob %s", srcBlobUrl)
	}
	defer srcBlobUrl.BreakLease(context.Background(), azblob.LeaseBreakNaturally, azblob.ModifiedAccessConditions{})

	srcBlobLeaseId := leaseResp.LeaseID()

	copyResp, err := dstBlobUrl.StartCopyFromURL(
		context.Background(),
		srcBlobUrl.URL(),
		metadata,
		azblob.ModifiedAccessConditions{},
		azblob.BlobAccessConditions{
			LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: dstBlobLeaseId},
		},
		azblob.DefaultAccessTier,
		nil)
	if err != nil {
		return fmt.Errorf("error copying %s -> %s in %s: %s", src, dst, storage, err)
	}

	copyStatus := copyResp.CopyStatus()
	for {
		if copyStatus == azblob.CopyStatusSuccess {
			if move {
				_, err = srcBlobUrl.Delete(
					context.Background(),
					azblob.DeleteSnapshotsOptionNone,
					azblob.BlobAccessConditions{
						LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: srcBlobLeaseId},
					})
				return err
			} else {
				return nil
			}
		} else if copyStatus == azblob.CopyStatusPending {
			time.Sleep(1 * time.Second)
			blobPropsResp, err := dstBlobUrl.GetProperties(
				context.Background(),
				azblob.BlobAccessConditions{LeaseAccessConditions: azblob.LeaseAccessConditions{LeaseID: srcBlobLeaseId}},
				azblob.ClientProvidedKeyOptions{})
			if err != nil {
				return fmt.Errorf("error getting destination blob properties %s", dstBlobUrl)
			}
			copyStatus = blobPropsResp.CopyStatus()

			_, err = dstBlobUrl.RenewLease(context.Background(), dstBlobLeaseId, azblob.ModifiedAccessConditions{})
			if err != nil {
				return fmt.Errorf("error renewing destination blob lease %s", dstBlobUrl)
			}
			_, err = srcBlobUrl.RenewLease(context.Background(), srcBlobLeaseId, azblob.ModifiedAccessConditions{})
			if err != nil {
				return fmt.Errorf("error renewing source blob lease %s", srcBlobUrl)
			}
		}
		return fmt.Errorf("error copying %s -> %s in %s: %s", dst, src, storage, copyStatus)
	}
}

// RenameFile renames (moves) file
func (storage *PublishedStorage) RenameFile(oldName, newName string) error {
	return storage.internalCopyOrMoveBlob(oldName, newName, nil, true)
}

// SymLink creates a copy of src file and adds link information as meta data
func (storage *PublishedStorage) SymLink(src string, dst string) error {
	return storage.internalCopyOrMoveBlob(src, dst, azblob.Metadata{"SymLink": src}, false)
}

// HardLink using symlink functionality as hard links do not exist
func (storage *PublishedStorage) HardLink(src string, dst string) error {
	return storage.SymLink(src, dst)
}

// FileExists returns true if path exists
func (storage *PublishedStorage) FileExists(path string) (bool, error) {
	blob := storage.container.NewBlobURL(filepath.Join(storage.prefix, path))
	resp, err := blob.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return false, err
	} else if resp.StatusCode() == http.StatusNotFound {
		return false, nil
	} else if resp.StatusCode() == http.StatusOK {
		return true, nil
	} else {
		return false, fmt.Errorf("error checking if blob %s exists %d", blob, resp.StatusCode())
	}
}

// ReadLink returns the symbolic link pointed to by path.
// This simply reads text file created with SymLink
func (storage *PublishedStorage) ReadLink(path string) (string, error) {
	blob := storage.container.NewBlobURL(filepath.Join(storage.prefix, path))
	resp, err := blob.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	} else if resp.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("error checking if blob %s exists %d", blob, resp.StatusCode())
	}
	return resp.NewMetadata()["SymLink"], nil
}
