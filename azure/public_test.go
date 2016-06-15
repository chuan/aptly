package azure

import (
	"crypto/rand"
	azure "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/smira/aptly/files"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"path/filepath"
)

type PublishedStorageSuite struct {
	accountName, accountKey  string
	storage, prefixedStorage *PublishedStorage
}

var _ = Suite(&PublishedStorageSuite{})

const testContainerPrefix = "aptlytest-"

func randContainer() string {
	return testContainerPrefix + randString(32-len(testContainerPrefix))
}

func randString(n int) string {
	if n <= 0 {
		panic("negative number")
	}
	const alphanum = "0123456789abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func (s *PublishedStorageSuite) SetUpSuite(c *C) {
	s.accountName = os.Getenv("AZURE_STORAGE_ACCOUNT")
	if s.accountName == "" {
		println("Please set the the following two environment variables to run the Azure storage tests.")
		println("  1. AZURE_STORAGE_ACCOUNT")
		println("  2. AZURE_STORAGE_ACCESS_KEY")
		c.Skip("AZURE_STORAGE_ACCOUNT not set.")
	}
	s.accountKey = os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if s.accountKey == "" {
		println("Please set the the following two environment variables to run the Azure storage tests.")
		println("  1. AZURE_STORAGE_ACCOUNT")
		println("  2. AZURE_STORAGE_ACCESS_KEY")
		c.Skip("AZURE_STORAGE_ACCESS_KEY not set.")
	}
}

func (s *PublishedStorageSuite) SetUpTest(c *C) {
	container := randContainer()
	prefix := "lala"

	var err error

	s.storage, err = NewPublishedStorage(s.accountName, s.accountKey, container, "")
	c.Assert(err, IsNil)
	c.Assert(s.storage.wasb.CreateContainer(container, azure.ContainerAccessTypePrivate), IsNil)

	s.prefixedStorage, err = NewPublishedStorage(s.accountName, s.accountKey, container, prefix)
	c.Assert(err, IsNil)
}

func (s *PublishedStorageSuite) TearDownTest(c *C) {
	c.Assert(s.storage.wasb.DeleteContainer(s.storage.container), IsNil)
}

func (s *PublishedStorageSuite) GetFile(c *C, path string) []byte {
	blob, err := s.storage.wasb.GetBlob(s.storage.container, path)
	defer blob.Close()
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(blob)
	c.Assert(err, IsNil)
	return data
}

func (s *PublishedStorageSuite) TestPutFile(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("Welcome to Azure!"), 0644)
	c.Assert(err, IsNil)

	err = s.storage.PutFile("a/b.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "a/b.txt"), DeepEquals, []byte("Welcome to Azure!"))

	err = s.prefixedStorage.PutFile("a/b.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, filepath.Join(s.prefixedStorage.prefix, "a/b.txt")), DeepEquals, []byte("Welcome to Azure!"))
}

func (s *PublishedStorageSuite) TestFilelist(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("Welcome to Azure!"), 0644)
	c.Assert(err, IsNil)

	paths := []string{"a", "b", "c", "testa", "test/a", "test/b", "lala/a", "lala/b", "lala/c"}
	for _, path := range paths {
		err = s.storage.PutFile(path, filepath.Join(dir, "a"))
		c.Check(err, IsNil)
	}

	list, err := s.storage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c", "lala/a", "lala/b", "lala/c", "test/a", "test/b", "testa"})

	list, err = s.storage.Filelist("test")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b"})

	list, err = s.storage.Filelist("test2")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{})

	list, err = s.prefixedStorage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c"})
}

func (s *PublishedStorageSuite) TestRemove(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("Welcome to Azure!"), 0644)
	c.Assert(err, IsNil)

	err = s.storage.PutFile("a/b.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	err = s.storage.Remove("a/b.txt")
	c.Check(err, IsNil)

	_, err = s.storage.wasb.GetBlob(s.storage.container, "a/b.txt")
	c.Check(err, ErrorMatches, "(?m).*ErrorCode=BlobNotFound.*")
}

func (s *PublishedStorageSuite) TestRemoveDirs(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("Welcome to Azure!"), 0644)
	c.Assert(err, IsNil)

	paths := []string{"a", "b", "c", "testa", "test/a", "test/b", "lala/ab", "lala/c", "lala/c"}
	for _, path := range paths {
		err = s.storage.PutFile(path, filepath.Join(dir, "a"))
		c.Check(err, IsNil)
	}

	err = s.storage.RemoveDirs("test", nil)
	c.Check(err, IsNil)

	list, err := s.storage.Filelist("")
	c.Check(err, IsNil)
	c.Check(list, DeepEquals, []string{"a", "b", "c", "lala/ab", "lala/c", "testa"})
}

func (s *PublishedStorageSuite) TestRenameFile(c *C) {
	dir := c.MkDir()
	err := ioutil.WriteFile(filepath.Join(dir, "a"), []byte("Welcome to Azure!"), 0644)
	c.Assert(err, IsNil)

	err = s.storage.PutFile("source.txt", filepath.Join(dir, "a"))
	c.Check(err, IsNil)

	err = s.storage.RenameFile("source.txt", "dest.txt")
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "dest.txt"), DeepEquals, []byte("Welcome to Azure!"))

	_, err = s.storage.wasb.GetBlob(s.storage.container, "source.txt")
	c.Check(err, ErrorMatches, "(?m).*ErrorCode=BlobNotFound.*")
}

func (s *PublishedStorageSuite) TestLinkFromPool(c *C) {
	root := c.MkDir()
	pool := files.NewPackagePool(root)

	sourcePath := filepath.Join(root, "pool/c1/df/mars-invaders_1.03.deb")
	err := os.MkdirAll(filepath.Dir(sourcePath), 0755)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(sourcePath, []byte("Contents"), 0644)
	c.Assert(err, IsNil)

	sourcePath2 := filepath.Join(root, "pool/e9/df/mars-invaders_1.03.deb")
	err = os.MkdirAll(filepath.Dir(sourcePath2), 0755)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(sourcePath2, []byte("Spam"), 0644)
	c.Assert(err, IsNil)

	// first link from pool
	err = s.storage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), pool, sourcePath, "c1df1da7a1ce305a3b60af9d5733ac1d", false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// duplicate link from pool
	err = s.storage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), pool, sourcePath, "c1df1da7a1ce305a3b60af9d5733ac1d", false)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// link from pool with conflict
	err = s.storage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), pool, sourcePath2, "e9dfd31cc505d51fc26975250750deab", false)
	c.Check(err, ErrorMatches, ".*file already exists and is different.*")

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Contents"))

	// link from pool with conflict and force
	err = s.storage.LinkFromPool(filepath.Join("", "pool", "main", "m/mars-invaders"), pool, sourcePath2, "e9dfd31cc505d51fc26975250750deab", true)
	c.Check(err, IsNil)

	c.Check(s.GetFile(c, "pool/main/m/mars-invaders/mars-invaders_1.03.deb"), DeepEquals, []byte("Spam"))
}
