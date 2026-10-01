package chunk2

import (
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// chunkObject is a virtual file merged from several flat chunk files that live
// directly inside the parent directory.
type chunkObject struct {
	model.Object
	// chunkSizes maps chunk index -> chunk size.
	// The last element is the tail chunk, whose size may differ from PartSize.
	chunkSizes []int64
	// hashes records the hash sidecars stored next to the chunks. Because a
	// sidecar name embeds the logical file name, operations that rename, move,
	// copy or delete this file must carry these sidecars along.
	hashes map[*utils.HashType]string
}
