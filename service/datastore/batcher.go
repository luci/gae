// Copyright 2016 The LUCI Authors.
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
	"fmt"
	"math"

	"go.chromium.org/luci/common/errors"
	"go.chromium.org/luci/common/sync/parallel"

	"golang.org/x/net/context"
)

// QueryBatchCallback is called in between batch query iterations. If the
// callback returns an error, the error will be returned by the top-level
// operation, and no further batches will be executed.
//
// When querying, the Callback will be executed in between query operations,
// meaning that the time consumed by the callback will not run the risk of
// timing out any individual query.
//
// QueryBatchCallback can be installed using WithQueryBatchCallback.
type QueryBatchCallback func(context.Context) error

func applyBatchFilter(c context.Context, rds RawInterface) RawInterface {
	constraints := rds.Constraints()

	queryBatchSize := getQueryBatchSize(c)
	if queryBatchSize <= 0 {
		queryBatchSize = constraints.QueryBatchSize
	}

	batchingEnabled, batchingSpecified := getBatching(c)
	return &batchFilter{
		RawInterface:       rds,
		ic:                 c,
		constraints:        rds.Constraints(),
		batchingSpecified:  batchingSpecified,
		batchingEnabled:    batchingEnabled,
		queryBatchSize:     queryBatchSize,
		queryBatchCallback: queryBatchCallback(c),
	}
}

type batchFilter struct {
	RawInterface

	ic                 context.Context
	constraints        Constraints
	batchingSpecified  bool
	batchingEnabled    bool
	queryBatchSize     int
	queryBatchCallback QueryBatchCallback
}

func (bf *batchFilter) Run(fq *FinalizedQuery, cb RawRunCB) error {
	// Batching is enabled by default.
	batching := bf.batchingEnabled || !bf.batchingSpecified

	// If we're running a projection query and batching is not explicitly
	// specified, it is disabled.
	if len(fq.Project()) > 0 && !bf.batchingSpecified {
		batching = false
	}

	// Determine batch size.
	switch {
	case !batching, bf.queryBatchSize <= 0:
		return bf.RawInterface.Run(fq, cb)
	case bf.queryBatchSize > math.MaxInt32:
		return errors.New("batch size must fit in int32")
	}
	bs := int32(bf.queryBatchSize)
	limit, hasLimit := fq.Limit()

	// Install an intermediate callback so we can iteratively batch.
	var cursor Cursor
	for {
		iterQuery := fq.Original()
		if cursor != nil {
			iterQuery = iterQuery.Start(cursor)
			cursor = nil
		}
		iterLimit := bs
		if hasLimit && limit < iterLimit {
			iterLimit = limit
		}
		iterQuery = iterQuery.Limit(iterLimit)

		iterFinalizedQuery, err := iterQuery.Finalize()
		if err != nil {
			panic(fmt.Errorf("failed to finalize internal query: %v", err))
		}

		count := int32(0)
		err = bf.RawInterface.Run(iterFinalizedQuery, func(key *Key, val PropertyMap, getCursor CursorCB) error {
			if cursor != nil {
				// We're iterating past our batch size, which should never happen, since
				// we set a limit. This will only happen when our inner RawInterface
				// fails to honor the limit that we set.
				panic(fmt.Errorf("iterating past batch size"))
			}

			if err := cb(key, val, getCursor); err != nil {
				return err
			}

			// If this is the last entry in our batch, get the cursor.
			count++
			if count >= bs {
				if cursor, err = getCursor(); err != nil {
					return fmt.Errorf("failed to get cursor: %v", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		// If we have no cursor, we're done.
		if cursor == nil {
			break
		}

		// Reduce our limit for the next round.
		if hasLimit {
			limit -= count
			if limit <= 0 {
				break
			}
		}

		// Execute our callback(s).
		if bf.queryBatchCallback != nil {
			if err := bf.queryBatchCallback(bf.ic); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bf *batchFilter) GetMulti(keys []*Key, meta MultiMetaGetter, cb GetMultiCB) error {
	return bf.batchParallel(len(keys), bf.constraints.MaxPutSize, func(offset, count int) error {
		return bf.RawInterface.GetMulti(keys[offset:offset+count], meta, func(idx int, val PropertyMap, err error) error {
			return cb(offset+idx, val, err)
		})
	})
}

func (bf *batchFilter) PutMulti(keys []*Key, vals []PropertyMap, cb NewKeyCB) error {
	return bf.batchParallel(len(vals), bf.constraints.MaxPutSize, func(offset, count int) error {
		return bf.RawInterface.PutMulti(keys[offset:offset+count], vals[offset:offset+count], func(idx int, key *Key, err error) error {
			return cb(offset+idx, key, err)
		})
	})
}

func (bf *batchFilter) DeleteMulti(keys []*Key, cb DeleteMultiCB) error {
	return bf.batchParallel(len(keys), bf.constraints.MaxPutSize, func(offset, count int) error {
		return bf.RawInterface.DeleteMulti(keys[offset:offset+count], func(idx int, err error) error {
			return cb(offset+idx, err)
		})
	})
}

func (bf *batchFilter) batchParallel(count, batch int, cb func(offset, count int) error) error {
	// If no batch size is defined, do everything in a single batch.
	if batch <= 0 {
		return cb(0, count)
	}

	// We batch by default unless the user specifies otherwise.
	batching := (bf.batchingEnabled || !bf.batchingSpecified) && batch > 0

	// If batching is disabled, we will skip goroutines and do everything in a
	// single batch.
	if !batching {
		if batch > 0 && count > batch {
			return errors.Reason("batching is disabled, and size (%d) exceeds maximum (%d)", count, batch).Err()
		}
		return cb(0, count)
	}

	// Dispatch our batches in parallel.
	return parallel.FanOutIn(func(workC chan<- func() error) {
		for i := 0; i < count; {
			offset := i
			size := count - i
			if size > batch {
				size = batch
			}

			workC <- func() error {
				return filterStop(cb(offset, size))
			}

			i += size
		}
	})
}
