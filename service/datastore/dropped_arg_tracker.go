// Copyright 2020 The LUCI Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"sort"

	"go.chromium.org/luci/common/errors"
)

// DroppedArgTracker is used to track dropping items from Keys as well as meta
// and/or PropertyMap arrays from one layer of the RawInterface to the next.
//
// If you're not writing a datastore backend implementation (like
// "go.chromium.org/gae/impl/*"), then you can ignore this type.
//
// For example, say your GetMulti method was passed 4 arguments, but one of them
// was bad. DroppedArgTracker would allow you to "drop" the bad entry, and then
// synthesize new keys/meta/values arrays excluding the bad entry. You could
// then map from the new arrays back to the indexes of the original arrays.
//
// This DroppedArgTracker will do no allocations if you don't end up dropping
// any arguments (so in the 'good' case, there are zero allocations).
//
// Example:
//
//    Say we're given a list of arguments which look like ("_" means a bad value
//    that we drop):
//
//     input: A B _ C D _ _ E
//      Idxs: 0 1 2 3 4 5 6 7
//   dropped:     2     5 6
//
//  DropKeys(input): A B C D E
//                   0 1 2 3 4
//
//  OriginalIndex(0) -> 0
//  OriginalIndex(1) -> 1
//  OriginalIndex(2) -> 3
//  OriginalIndex(3) -> 4
//  OriginalIndex(4) -> 7
//
// Methods on this type are NOT goroutine safe.
type DroppedArgTracker []int

// MarkForRemoval tracks `originalIndex` for removal when `Drop*` methods
// are called.
//
// N is a size hint for the maximum number of entries that `dat` could have. If
// `dat` has a capacity of > N, it will be allocated to N.
//
// If called with N == len(args) and originalIndex is always increasing, then
// this will only do one allocation for the life of this DroppedArgTracker, and
// each MarkForRemoval will only cost a single slice append. If called out of
// order, or with a bad value of N, this will do more allocations and will do
// a binary search on each call.
func (dat *DroppedArgTracker) MarkForRemoval(originalIndex, N int) {
	datLen := len(*dat)

	if cap(*dat) < N {
		newDat := make([]int, datLen, N)
		copy(newDat, *dat)
		*dat = newDat
	}

	// most uses will insert linearly, so do a quick check of the max element to
	// see if originalIndex is larger and then do a simple append.
	if datLen == 0 || originalIndex > (*dat)[datLen-1] {
		*dat = append(*dat, originalIndex)
		return
	}

	// Otherwise, search for the correct location and insert it
	insIdx := sort.SearchInts(*dat, originalIndex)
	if insIdx < datLen && (*dat)[insIdx] == originalIndex {
		return
	}
	*dat = append(*dat, 0)
	copy((*dat)[insIdx+1:], (*dat)[insIdx:])
	(*dat)[insIdx] = originalIndex
}

// MarkNilKeys is a helper method which calls MarkForRemoval for each nil key.
func (dat *DroppedArgTracker) MarkNilKeys(keys []*Key) {
	for idx, k := range keys {
		if k == nil {
			dat.MarkForRemoval(idx, len(keys))
		}
	}
}

// MarkNilKeysMeta is a helper method which calls MarkForRemoval for each nil
// key or meta.
func (dat *DroppedArgTracker) MarkNilKeysMeta(keys []*Key, meta MultiMetaGetter) {
	for idx, k := range keys {
		if k == nil || meta[idx] == nil {
			dat.MarkForRemoval(idx, len(keys))
		}
	}
}

// MarkNilKeysVals is a helper method which calls MarkForRemoval for each nil
// key or value.
func (dat *DroppedArgTracker) MarkNilKeysVals(keys []*Key, vals []PropertyMap) {
	for idx, k := range keys {
		if k == nil || vals[idx] == nil {
			dat.MarkForRemoval(idx, len(keys))
		}
	}
}

// If `dat` has a positive length, this will invoke `init` once, followed by
// `include` for every non-overlapping (i, j) range less than N which doesn't
// include any elements indicated with MarkForRemoval.
//
// If `dat` contains a removed index larger than N, this panics.
func (dat DroppedArgTracker) mustCompress(N int, init func(), include func(i, j int)) {
	if len(dat) == 0 || N == 0 {
		return
	}

	if largestDropIdx := dat[len(dat)-1]; largestDropIdx >= N {
		panic(errors.Reason(
			"DroppedArgTracker has out of bound index: %d >= %d ",
			largestDropIdx, N,
		).Err())
	}

	init()
	prevDropped := 0
	for _, droppedIdx := range dat {
		if droppedIdx > prevDropped {
			include(prevDropped, droppedIdx)
		}
		prevDropped = droppedIdx + 1
	}
	include(prevDropped, N)
}

// DropKeys returns a compressed version of `keys`, dropping all elements which
// were marked with MarkForRemoval.
func (dat DroppedArgTracker) DropKeys(keys []*Key) []*Key {
	newKeys := keys

	init := func() {
		newKeys = make([]*Key, 0, len(keys)-len(dat))
	}
	include := func(i, j int) {
		newKeys = append(newKeys, keys[i:j]...)
	}

	dat.mustCompress(len(keys), init, include)

	return newKeys
}

// DropKeysAndMeta returns a compressed version of `keys` and `meta`, dropping
// all elements which were marked with MarkForRemoval.
//
// `keys` and `meta` must have the same lengths.
func (dat DroppedArgTracker) DropKeysAndMeta(keys []*Key, meta MultiMetaGetter) ([]*Key, MultiMetaGetter) {
	newKeys := keys
	newMeta := meta

	// MultiMetaGetter is special and frequently is len 0 with non-nil keys, so we
	// just keep it empty.

	init := func() {
		newKeys = make([]*Key, 0, len(keys)-len(dat))
		if len(meta) > 0 {
			newMeta = make(MultiMetaGetter, 0, len(keys)-len(dat))
		}
	}
	include := func(i, j int) {
		newKeys = append(newKeys, keys[i:j]...)
		if len(meta) > 0 {
			newMeta = append(newMeta, meta[i:j]...)
		}
	}

	dat.mustCompress(len(keys), init, include)

	return newKeys, newMeta
}

// DropKeysAndVals returns a compressed version of `keys` and `vals`, dropping
// all elements which were marked with MarkForRemoval.
//
// `keys` and `vals` must have the same lengths.
func (dat DroppedArgTracker) DropKeysAndVals(keys []*Key, vals []PropertyMap) ([]*Key, []PropertyMap) {
	newKeys := keys
	newVals := vals

	if len(keys) != len(vals) {
		panic(errors.Reason(
			"DroppedArgTracker.DropKeysAndVals: mismatched lengths: %d vs %d",
			len(keys), len(vals),
		).Err())
	}

	init := func() {
		newKeys = make([]*Key, 0, len(keys)-len(dat))
		newVals = make([]PropertyMap, 0, len(keys)-len(dat))
	}
	include := func(i, j int) {
		newKeys = append(newKeys, keys[i:j]...)
		newVals = append(newVals, vals[i:j]...)
	}

	dat.mustCompress(len(keys), init, include)

	return newKeys, newVals
}

// OriginalIndex maps from an index into the array(s) returned from MustDrop
// back to the corresponding index in the original arrays.
func (dat DroppedArgTracker) OriginalIndex(reducedIndex int) int {
	for _, missingKey := range dat {
		if reducedIndex < missingKey {
			return reducedIndex
		}
		reducedIndex++
	}
	return reducedIndex
}
