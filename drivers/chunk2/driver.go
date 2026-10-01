package chunk2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdpath "path"
	"sort"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/sign"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
)

type Chunk2 struct {
	model.Storage
	Addition
}

func (d *Chunk2) Config() driver.Config {
	return config
}

func (d *Chunk2) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Chunk2) Init(ctx context.Context) error {
	if d.PartSize <= 0 {
		return errors.New("part size must be positive")
	}
	if len(d.ChunkPrefix) <= 0 {
		return errors.New("chunk file prefix must not be empty")
	}
	if strings.ContainsAny(d.ChunkPrefix, "/\\") {
		return errors.New("chunk file prefix must not contain path separator")
	}
	d.RemotePath = utils.FixAndCleanPath(d.RemotePath)
	return nil
}

func (d *Chunk2) Drop(ctx context.Context) error {
	return nil
}

func (*Chunk2) GetRootPath() string {
	return ""
}

// ---------------------------------------------------------------------------
// naming helpers
//
// Flat layout: every chunk of a logical file lives in the SAME directory as the
// file would have, named ChunkPrefix + fileName + "_" + index (+ CustomExt).
//
//	/dir/test.txt_1      -> logical file "test.txt", chunk 1
//	/dir/[chunk]a.mkv_0  -> logical file "a.mkv", chunk 0
//	/dir/hash_md5_xxx    -> hash sidecar (no chunk index)
//
// The index is always the LAST "_"-separated field, so a file whose own name
// contains "_" (e.g. "a_9" -> "a_9_1") still parses unambiguously.
// ---------------------------------------------------------------------------

// getPartName builds the physical name of chunk `part` for logical name `name`.
func (d *Chunk2) getPartName(name string, part int) string {
	return fmt.Sprintf("%s%s_%d%s", d.ChunkPrefix, name, part, d.CustomExt)
}

// getHashName builds the sidecar name that stores one hash value for `name`.
//
//	ChunkPrefix + fileName + "_" + hashType + "_" + hashValue (+ CustomExt)
//	e.g. "[chunk]test.txt_md5_d41d8cd98f00b204e9800998ecf8427e"
//
// Because the logical file name is embedded, a sidecar is unambiguously
// attributable to one file even when a directory holds several chunked files.
func (d *Chunk2) getHashName(name, htName, value string) string {
	return fmt.Sprintf("%s%s_%s_%s%s", d.ChunkPrefix, name, htName, value, d.CustomExt)
}

// parseChunkName splits a raw physical name into (logicalName, partIndex).
// It returns ok=false for anything that is not a chunk file of this driver.
func (d *Chunk2) parseChunkName(rawName string) (name string, part int, ok bool) {
	after, found := strings.CutPrefix(rawName, d.ChunkPrefix)
	if !found {
		return "", 0, false
	}
	// strip the optional custom extension
	if d.CustomExt != "" {
		trimmed, found := strings.CutSuffix(after, d.CustomExt)
		if !found {
			return "", 0, false
		}
		after = trimmed
	}
	// the index is the last "_"-separated field
	idx := strings.LastIndex(after, "_")
	if idx <= 0 || idx == len(after)-1 {
		// no separator, or nothing after it -> not a chunk file
		return "", 0, false
	}
	part, err := strconv.Atoi(after[idx+1:])
	if err != nil || part < 0 {
		return "", 0, false
	}
	name = after[:idx]
	if name == "" {
		return "", 0, false
	}
	return name, part, true
}

// parseHashName splits a raw physical name into (logicalName, hashType, value).
//
//	"[chunk]test.txt_md5_<hex>" -> ("test.txt", "md5", "<hex>")
//
// The hash type is a known registered name, and the value must be a digest of
// that algorithm's exact width. Together these keep a chunk file (whose own name
// may end in "_<digits>") from being mistaken for a sidecar, and reject
// leftovers from other naming schemes.
func (d *Chunk2) parseHashName(rawName string) (name, htName, value string, ok bool) {
	after, found := strings.CutPrefix(rawName, d.ChunkPrefix)
	if !found {
		return "", "", "", false
	}
	if d.CustomExt != "" {
		trimmed, found := strings.CutSuffix(after, d.CustomExt)
		if !found {
			return "", "", "", false
		}
		after = trimmed
	}
	// value is the last field, hash type is the one before it
	last := strings.LastIndex(after, "_")
	if last <= 0 || last == len(after)-1 {
		return "", "", "", false
	}
	value = after[last+1:]
	head := after[:last]
	sep := strings.LastIndex(head, "_")
	if sep <= 0 {
		return "", "", "", false
	}
	htName = head[sep+1:]
	ht, known := utils.GetHashByName(htName)
	if !known {
		return "", "", "", false
	}
	// Validate the value against the algorithm: a real digest is exactly
	// Width hex characters. This rejects leftovers from other naming schemes
	// (e.g. a bare "[chunk]hash_md5_deadbeef") that would otherwise look like
	// a sidecar of a file literally named "hash".
	if len(value) != ht.Width || !isHex(value) {
		return "", "", "", false
	}
	name = head[:sep]
	if name == "" {
		return "", "", "", false
	}
	return name, htName, value, true
}

// isHex reports whether s consists solely of lowercase or uppercase hex digits.
func isHex(s string) bool {
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

// collectChunks scans the objects of ONE flat directory and groups every chunk
// file by its logical file name.
type chunkAgg struct {
	chunkSizes []int64
	hashes     map[*utils.HashType]string
	totalSize  int64
	first      model.Obj // chunk 0, used for timestamps
}

// buildChunkAgg turns a directory listing into per-logical-file chunk info.
//
// Hash sidecars embed the file name, so each one is attributed to its own file
// and no cross-file ambiguity exists.
func (d *Chunk2) buildChunkAgg(objs []model.Obj) map[string]*chunkAgg {
	aggs := make(map[string]*chunkAgg)
	get := func(name string) *chunkAgg {
		a, ok := aggs[name]
		if !ok {
			// chunk 0 defaults to -1 so that an empty file is distinguishable
			// from a file whose chunk 0 is missing.
			a = &chunkAgg{chunkSizes: []int64{-1}}
			aggs[name] = a
		}
		return a
	}
	for _, o := range objs {
		if o.IsDir() {
			continue
		}
		rawName := o.GetName()
		if !strings.HasPrefix(rawName, d.ChunkPrefix) {
			continue
		}
		if name, hn, value, ok := d.parseHashName(rawName); ok {
			if ht, known := utils.GetHashByName(hn); known {
				a := get(name)
				if a.hashes == nil {
					a.hashes = make(map[*utils.HashType]string)
				}
				a.hashes[ht] = value
			}
			continue
		}
		name, part, ok := d.parseChunkName(rawName)
		if !ok {
			continue
		}
		a := get(name)
		a.totalSize += o.GetSize()
		switch {
		case len(a.chunkSizes) > part:
			a.chunkSizes[part] = o.GetSize()
		case len(a.chunkSizes) == part:
			a.chunkSizes = append(a.chunkSizes, o.GetSize())
		default:
			grown := make([]int64, part+1)
			copy(grown, a.chunkSizes)
			a.chunkSizes = grown
			a.chunkSizes[part] = o.GetSize()
		}
		if part == 0 {
			a.first = o
		}
	}
	return aggs
}

// complete reports whether every chunk of the file is present.
// Chunk 0 must exist; intermediate chunks must be non-zero; the tail chunk is
// allowed to be any size because it legitimately holds the remainder.
func complete(chunkSizes []int64) bool {
	if len(chunkSizes) == 0 || chunkSizes[0] == -1 {
		return false
	}
	for i, l := 1, len(chunkSizes)-1; i < l; i++ {
		if chunkSizes[i] == 0 {
			return false
		}
	}
	return true
}

// newObjFromAgg materialises a virtual file object from aggregated chunk info.
func (d *Chunk2) newObjFromAgg(path, name string, a *chunkAgg) *chunkObject {
	obj := &chunkObject{
		Object: model.Object{
			Path: path,
			Name: name,
			Size: a.totalSize,
		},
		chunkSizes: a.chunkSizes,
		hashes:     a.hashes,
	}
	if a.first != nil {
		obj.Modified = a.first.ModTime()
		obj.Ctime = a.first.CreateTime()
	}
	if len(a.hashes) > 0 {
		obj.HashInfo = utils.NewHashInfoByMap(a.hashes)
	}
	return obj
}

// Get resolves a single path. A real remote object always wins; otherwise the
// path is looked up as a logical file assembled from flat chunks.
func (d *Chunk2) Get(ctx context.Context, path string) (model.Obj, error) {
	remoteStorage, remoteActualPath, err := op.GetStorageAndActualPath(d.RemotePath)
	if err != nil {
		return nil, err
	}
	remoteActualPath = stdpath.Join(remoteActualPath, path)
	if remoteObj, err := op.Get(ctx, remoteStorage, remoteActualPath); err == nil {
		return &model.Object{
			Path:     path,
			Name:     remoteObj.GetName(),
			Size:     remoteObj.GetSize(),
			Modified: remoteObj.ModTime(),
			IsFolder: remoteObj.IsDir(),
			HashInfo: remoteObj.GetHash(),
		}, nil
	}

	// not a real object: try to assemble it from flat chunks in its parent dir
	remoteActualDir, name := stdpath.Split(remoteActualPath)
	name = strings.TrimSuffix(name, "/")
	objs, err := op.List(ctx, remoteStorage, strings.TrimSuffix(remoteActualDir, "/"), model.ListArgs{})
	if err != nil {
		return nil, err
	}
	aggs := d.buildChunkAgg(objs)
	a, ok := aggs[name]
	if !ok {
		// No chunk file of this name exists at all: the path genuinely does not
		// exist. Reporting a phantom folder here would make callers such as
		// op.MakeDir believe a directory is already present.
		return nil, errs.ObjectNotFound
	}

	reqDir, _ := stdpath.Split(path)
	if !complete(a.chunkSizes) {
		// Chunks exist but are incomplete: expose a browsable folder so the
		// pieces stay visible instead of surfacing a corrupt file.
		return &model.Object{
			Path:     stdpath.Join(reqDir, name),
			Name:     name,
			IsFolder: true,
			Modified: d.Modified,
		}, nil
	}
	return d.newObjFromAgg(stdpath.Join(reqDir, name), name, a), nil
}

// List lists a directory, hiding chunk files and re-exposing complete chunk
// groups as single virtual files.
func (d *Chunk2) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	remoteStorage, remoteActualPath, err := op.GetStorageAndActualPath(d.RemotePath)
	if err != nil {
		return nil, err
	}
	remoteActualDir := stdpath.Join(remoteActualPath, dir.GetPath())
	remoteObjs, err := op.List(ctx, remoteStorage, remoteActualDir, model.ListArgs{
		ReqPath: args.ReqPath,
		Refresh: args.Refresh,
	})
	if err != nil {
		return nil, err
	}

	aggs := d.buildChunkAgg(remoteObjs)
	result := make([]model.Obj, 0, len(remoteObjs))

	for _, obj := range remoteObjs {
		rawName := obj.GetName()

		// chunk files and hash sidecars are virtual: never listed directly
		if strings.HasPrefix(rawName, d.ChunkPrefix) {
			if _, _, ok := d.parseChunkName(rawName); ok {
				continue
			}
			if _, _, _, ok := d.parseHashName(rawName); ok {
				continue
			}
		}

		// a real object shadows any same-named chunk group
		if _, isChunkGroup := aggs[rawName]; isChunkGroup {
			delete(aggs, rawName)
		}

		if !d.ShowHidden && strings.HasPrefix(rawName, ".") {
			continue
		}
		thumb, ok := model.GetThumb(obj)
		objRes := model.Object{
			Name:     rawName,
			Size:     obj.GetSize(),
			Modified: obj.ModTime(),
			IsFolder: obj.IsDir(),
			HashInfo: obj.GetHash(),
		}
		if !ok {
			result = append(result, &objRes)
		} else {
			result = append(result, &model.ObjThumb{
				Object: objRes,
				Thumbnail: model.Thumbnail{
					Thumbnail: thumb,
				},
			})
		}
	}

	// group names are not real objects; a same-named real object already
	// removed its group above.
	// deterministic order over group names
	names := make([]string, 0, len(aggs))
	for name := range aggs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		a := aggs[name]
		if !complete(a.chunkSizes) {
			// incomplete -> expose as folder
			var totalSize int64
			if a.first != nil {
				totalSize = a.first.GetSize()
			}
			result = append(result, &model.Object{
				Name:     name,
				Size:     totalSize,
				Modified: d.Modified,
				IsFolder: true,
			})
			continue
		}
		objRes := model.Object{
			Name:     name,
			Size:     a.totalSize,
			Modified: d.Modified,
		}
		if a.first != nil {
			objRes.Modified = a.first.ModTime()
			objRes.Ctime = a.first.CreateTime()
		}
		if len(a.hashes) > 0 {
			objRes.HashInfo = utils.NewHashInfoByMap(a.hashes)
		}
		if !d.Thumbnail {
			result = append(result, &objRes)
		} else {
			thumbPath := stdpath.Join(args.ReqPath, ".thumbnails", name+".webp")
			thumb := fmt.Sprintf("%s/d%s?sign=%s",
				common.GetApiUrl(ctx),
				utils.EncodePath(thumbPath, true),
				sign.Sign(thumbPath))
			result = append(result, &model.ObjThumb{
				Object: objRes,
				Thumbnail: model.Thumbnail{
					Thumbnail: thumb,
				},
			})
		}
	}

	return result, nil
}

// Link returns a synthetic range reader that stitches flat chunks together.
func (d *Chunk2) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	remoteStorage, remoteActualPath, err := op.GetStorageAndActualPath(d.RemotePath)
	if err != nil {
		return nil, err
	}
	chunkFile, ok := file.(*chunkObject)
	remoteActualPath = stdpath.Join(remoteActualPath, file.GetPath())
	if !ok {
		l, _, err := op.Link(ctx, remoteStorage, remoteActualPath, args)
		if err != nil {
			return nil, err
		}
		return l.Clone(), nil
	}
	if !complete(chunkFile.chunkSizes) {
		for i, s := range chunkFile.chunkSizes {
			if i == len(chunkFile.chunkSizes)-1 {
				break
			}
			if i == 0 && s == -1 {
				return nil, fmt.Errorf("chunk part[%d] are missing", i)
			}
			if i > 0 && s == 0 {
				return nil, fmt.Errorf("chunk part[%d] are missing", i)
			}
		}
		return nil, errors.New("chunk file is incomplete")
	}

	fileSize := chunkFile.GetSize()
	dir := stdpath.Dir(remoteActualPath)
	baseName := chunkFile.GetName()

	mergedRrf := func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
		start := httpRange.Start
		length := httpRange.Length
		if length < 0 || start+length > fileSize {
			length = fileSize - start
		}
		if length == 0 {
			return io.NopCloser(strings.NewReader("")), nil
		}
		rs := make([]io.Reader, 0)
		cs := make(utils.Closers, 0)
		var (
			rc       io.ReadCloser
			readFrom bool
		)
		for idx, chunkSize := range chunkFile.chunkSizes {
			partPath := stdpath.Join(dir, d.getPartName(baseName, idx))
			if readFrom {
				l, o, err := op.Link(ctx, remoteStorage, partPath, args)
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				cs = append(cs, l)
				chunkSize2 := l.ContentLength
				if chunkSize2 <= 0 {
					chunkSize2 = o.GetSize()
				}
				if chunkSize2 != chunkSize {
					_ = cs.Close()
					return nil, fmt.Errorf("chunk part[%d] size not match", idx)
				}
				rrf, err := stream.GetRangeReaderFromLink(chunkSize2, l)
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				newLength := length - chunkSize2
				if newLength >= 0 {
					length = newLength
					rc, err = rrf.RangeRead(ctx, http_range.Range{Length: -1})
				} else {
					rc, err = rrf.RangeRead(ctx, http_range.Range{Length: length})
				}
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				rs = append(rs, rc)
				cs = append(cs, rc)
				if newLength <= 0 {
					return utils.ReadCloser{
						Reader: io.MultiReader(rs...),
						Closer: &cs,
					}, nil
				}
			} else if newStart := start - chunkSize; newStart >= 0 {
				start = newStart
			} else {
				l, o, err := op.Link(ctx, remoteStorage, partPath, args)
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				cs = append(cs, l)
				chunkSize2 := l.ContentLength
				if chunkSize2 <= 0 {
					chunkSize2 = o.GetSize()
				}
				if chunkSize2 != chunkSize {
					_ = cs.Close()
					return nil, fmt.Errorf("chunk part[%d] size not match", idx)
				}
				rrf, err := stream.GetRangeReaderFromLink(chunkSize2, l)
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				rc, err = rrf.RangeRead(ctx, http_range.Range{Start: start, Length: -1})
				if err != nil {
					_ = cs.Close()
					return nil, err
				}
				length -= chunkSize2 - start
				cs = append(cs, rc)
				if length <= 0 {
					return utils.ReadCloser{
						Reader: rc,
						Closer: &cs,
					}, nil
				}
				rs = append(rs, rc)
				readFrom = true
			}
		}
		return nil, fmt.Errorf("invalid range: start=%d,length=%d,fileSize=%d", httpRange.Start, httpRange.Length, fileSize)
	}
	return &model.Link{
		RangeReader: stream.RangeReaderFunc(mergedRrf),
	}, nil
}

func (d *Chunk2) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	path := stdpath.Join(d.RemotePath, parentDir.GetPath(), dirName)
	return fs.MakeDir(ctx, path)
}

func (d *Chunk2) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	src := stdpath.Join(d.RemotePath, srcObj.GetPath())
	dst := stdpath.Join(d.RemotePath, dstDir.GetPath())
	if _, ok := srcObj.(*chunkObject); ok {
		// move every physical chunk of the virtual file
		return d.moveChunks(ctx, srcObj, dst, false)
	}
	_, err := fs.Move(ctx, src, dst)
	return err
}

// physicalNames lists every physical object that makes up the virtual file
// `name`: all chunk indices (including the tail) plus the hash sidecars.
// `newName` lets callers compute the target names for a rename.
func (d *Chunk2) physicalNames(name, newName string, c *chunkObject) [][2]string {
	pairs := make([][2]string, 0, len(c.chunkSizes)+len(c.hashes))
	for i := range c.chunkSizes {
		pairs = append(pairs, [2]string{d.getPartName(name, i), d.getPartName(newName, i)})
	}
	for ht, value := range c.hashes {
		pairs = append(pairs, [2]string{
			d.getHashName(name, ht.Name, value),
			d.getHashName(newName, ht.Name, value),
		})
	}
	return pairs
}

// moveChunks relocates (or copies) all physical objects of a virtual file while
// mapping the logical name onto the destination directory.
func (d *Chunk2) moveChunks(ctx context.Context, srcObj model.Obj, dstDir string, keepSrc bool) error {
	srcDir := stdpath.Dir(stdpath.Join(d.RemotePath, srcObj.GetPath()))
	name := srcObj.GetName()
	for _, pair := range d.physicalNames(name, name, srcObj.(*chunkObject)) {
		srcPath := stdpath.Join(srcDir, pair[0])
		if keepSrc {
			if _, err := fs.Copy(ctx, srcPath, dstDir); err != nil {
				return err
			}
		} else {
			if _, err := fs.Move(ctx, srcPath, dstDir); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Chunk2) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	src := stdpath.Join(d.RemotePath, srcObj.GetPath())
	chunkFile, ok := srcObj.(*chunkObject)
	if !ok {
		return fs.Rename(ctx, src, newName)
	}
	// rename every chunk and sidecar, keeping the embedded logical name in sync
	dir := stdpath.Dir(src)
	for _, pair := range d.physicalNames(srcObj.GetName(), newName, chunkFile) {
		if err := fs.Rename(ctx, stdpath.Join(dir, pair[0]), pair[1]); err != nil {
			return err
		}
	}
	return nil
}

func (d *Chunk2) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	src := stdpath.Join(d.RemotePath, srcObj.GetPath())
	dst := stdpath.Join(d.RemotePath, dstDir.GetPath())
	if _, ok := srcObj.(*chunkObject); ok {
		// srcDir==dstDir would copy a file onto itself; skip that case
		if stdpath.Dir(src) == stdpath.Clean(dst) {
			return nil
		}
		return d.moveChunks(ctx, srcObj, dst, true)
	}
	_, err := fs.Copy(ctx, src, dst)
	return err
}

func (d *Chunk2) Remove(ctx context.Context, obj model.Obj) error {
	chunkFile, ok := obj.(*chunkObject)
	if !ok {
		return fs.Remove(ctx, stdpath.Join(d.RemotePath, obj.GetPath()))
	}
	dir := stdpath.Dir(stdpath.Join(d.RemotePath, obj.GetPath()))
	for _, pair := range d.physicalNames(obj.GetName(), obj.GetName(), chunkFile) {
		if err := fs.Remove(ctx, stdpath.Join(dir, pair[0])); err != nil {
			return err
		}
	}
	return nil
}

// Put splits the incoming stream into flat chunks named
// ChunkPrefix + fileName + "_" + index.
func (d *Chunk2) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	remoteStorage, remoteActualPath, err := op.GetStorageAndActualPath(d.RemotePath)
	if err != nil {
		return err
	}
	if (d.Thumbnail && dstDir.GetName() == ".thumbnails") || (d.ChunkLargeFileOnly && file.GetSize() <= d.PartSize) {
		return op.Put(ctx, remoteStorage, stdpath.Join(remoteActualPath, dstDir.GetPath()), file, up)
	}
	upReader := &driver.ReaderUpdatingProgress{
		Reader:         file,
		UpdateProgress: up,
	}
	dst := stdpath.Join(remoteActualPath, dstDir.GetPath())
	skipHookCtx := context.WithValue(ctx, conf.SkipHookKey, struct{}{})
	if d.StoreHash {
		for ht, value := range file.GetHash().All() {
			_ = op.Put(skipHookCtx, remoteStorage, dst, &stream.FileStream{
				Obj: &model.Object{
					Name:     d.getHashName(file.GetName(), ht.Name, value),
					Size:     1,
					Modified: file.ModTime(),
				},
				Mimetype: "application/octet-stream",
				Reader:   bytes.NewReader([]byte{0}), // 兼容不支持空文件的驱动
			}, nil)
		}
	}
	fullPartCount := int(file.GetSize() / d.PartSize)
	tailSize := file.GetSize() % d.PartSize
	if tailSize == 0 && fullPartCount > 0 {
		fullPartCount--
		tailSize = d.PartSize
	}
	// cleanup removes every physical object this Put may have written:
	// all chunk indices (including the tail chunk) and the hash sidecars.
	// It is safe to call unconditionally: removing a not-yet-written chunk
	// simply fails and is ignored.
	cleanup := func() {
		base := stdpath.Join(d.RemotePath, dstDir.GetPath())
		for i := 0; i <= fullPartCount; i++ {
			_ = fs.Remove(ctx, stdpath.Join(base, d.getPartName(file.GetName(), i)))
		}
		for ht, value := range file.GetHash().All() {
			_ = fs.Remove(ctx, stdpath.Join(base, d.getHashName(file.GetName(), ht.Name, value)))
		}
	}
	partIndex := 0
	for partIndex < fullPartCount {
		err = op.Put(skipHookCtx, remoteStorage, dst, &stream.FileStream{
			Obj: &model.Object{
				Name:     d.getPartName(file.GetName(), partIndex),
				Size:     d.PartSize,
				Modified: file.ModTime(),
			},
			Mimetype: file.GetMimetype(),
			Reader:   io.LimitReader(upReader, d.PartSize),
		}, nil)
		if err != nil {
			cleanup()
			return err
		}
		partIndex++
	}
	err = op.Put(ctx, remoteStorage, dst, &stream.FileStream{
		Obj: &model.Object{
			Name:     d.getPartName(file.GetName(), fullPartCount),
			Size:     tailSize,
			Modified: file.ModTime(),
		},
		Mimetype: file.GetMimetype(),
		Reader:   upReader,
	}, nil)
	if err != nil {
		cleanup()
	}
	return err
}

func (d *Chunk2) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	remoteStorage, err := fs.GetStorage(d.RemotePath, &fs.GetStoragesArgs{})
	if err != nil {
		return nil, errs.NotImplement
	}
	remoteDetails, err := op.GetStorageDetails(ctx, remoteStorage)
	if err != nil {
		return nil, err
	}
	return &model.StorageDetails{
		DiskUsage: remoteDetails.DiskUsage,
	}, nil
}

var _ driver.Driver = (*Chunk2)(nil)
