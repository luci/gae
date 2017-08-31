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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"go.chromium.org/gae/service/info"

	"golang.org/x/net/context"

	. "github.com/smartystreets/goconvey/convey"
)

type counterFilter struct {
	run int32
	put int32
}

func (cf *counterFilter) filter() RawFilter {
	return func(c context.Context, rds RawInterface) RawInterface {
		return &counterFilterInst{
			RawInterface:  rds,
			counterFilter: cf,
		}
	}
}

type counterFilterInst struct {
	RawInterface
	*counterFilter
}

func (rc *counterFilterInst) Run(fq *FinalizedQuery, cb RawRunCB) error {
	atomic.AddInt32(&rc.run, 1)
	return rc.RawInterface.Run(fq, cb)
}

func (rc *counterFilterInst) PutMulti(keys []*Key, vals []PropertyMap, cb NewKeyCB) error {
	atomic.AddInt32(&rc.put, 1)
	return rc.RawInterface.PutMulti(keys, vals, cb)
}

func TestQueryBatch(t *testing.T) {
	t.Parallel()

	Convey("A testing datastore with a data set installed", t, func() {
		c := info.Set(context.Background(), fakeInfo{})

		fds := fakeDatastore{
			entities: 2048,
		}
		c = SetRawFactory(c, fds.factory())

		cf := counterFilter{}
		c = AddRawFilters(c, cf.filter())

		// Given query batch size, how many Run calls will be executed to pull
		// "total" results?
		expectedBatchRunCalls := func(total int32) int32 {
			if fds.constraints.QueryBatchSize <= 0 {
				return 1
			}
			exp := total / int32(fds.constraints.QueryBatchSize)
			if total%int32(fds.constraints.QueryBatchSize) != 0 {
				exp++
			}
			return exp
		}

		// Get all items in the query, then reset the counter.
		all := []*CommonStruct(nil)
		if err := GetAll(c, NewQuery(""), &all); err != nil {
			panic(err)
		}
		cf.run = 0

		for _, sizeBase := range []int{
			1,
			16,
			1024,
			2048,
		} {
			// Adjust to hit edge cases.
			for _, delta := range []int{-1, 0, 1} {
				batchSize := sizeBase + delta
				if batchSize <= 0 {
					continue
				}

				fds.constraints.QueryBatchSize = batchSize

				Convey(fmt.Sprintf(`With a batch filter size %d installed`, batchSize), func() {
					q := NewQuery("")

					Convey(`Can retrieve all of the items.`, func() {
						var got []*CommonStruct
						So(GetAll(c, q, &got), ShouldBeNil)
						So(got, ShouldResemble, all)

						// One call for every sub-query, plus one to hit Stop.
						runCalls := (len(all) / batchSize) + 1
						So(cf.run, ShouldEqual, runCalls)
					})

					Convey(`With a limit of 128, will retrieve 128 items.`, func() {
						const limit = 128
						q = q.Limit(int32(limit))

						var got []*CommonStruct
						So(GetAll(c, q, &got), ShouldBeNil)
						So(got, ShouldResemble, all[:limit])

						So(cf.run, ShouldEqual, expectedBatchRunCalls(limit))
					})
				})
			}
		}

		Convey(`Test iterative Run with cursors.`, func() {
			// This test will have a naive outer loop that fetches pages in large
			// increments using cursors. The outer loop will use the Batcher
			// internally, which will fetch smaller page sizes.
			testIterativeRun := func(rounds, outerFetchSize, batchSize int32) error {
				// Clear state and configure.
				cf.run = 0
				fds.entities = rounds * outerFetchSize
				fds.constraints.QueryBatchSize = int(batchSize)

				var (
					outerCount int32
					cursor     Cursor
				)
				for i := int32(0); i < rounds; i++ {
					// Fetch "outerFetchSize" items from our Batcher.
					q := NewQuery("").Limit(outerFetchSize)
					if cursor != nil {
						q = q.Start(cursor)
					}

					err := Run(c, q, func(v CommonStruct, getCursor CursorCB) (err error) {
						if v.Value != int64(outerCount) {
							return fmt.Errorf("query value doesn't match count (%d != %d)", v.Value, outerCount)
						}
						outerCount++

						// Retain our cursor from this round.
						cursor, err = getCursor()
						return
					})
					if err != nil {
						return err
					}
				}

				// Make sure we iterated through everything.
				if outerCount != fds.entities {
					return fmt.Errorf("query returned incomplete results (%d != %d)", outerCount, fds.entities)
				}

				// Make sure the appropriate number of real queries was executed.
				expectedRunCount := expectedBatchRunCalls(outerFetchSize) * rounds
				if cf.run != expectedRunCount {
					return fmt.Errorf("unexpected number of raw Run calls (%d != %d)", cf.run, expectedRunCount)
				}
				return nil
			}

			So(testIterativeRun(3, 2, 1), ShouldBeNil)
			So(testIterativeRun(3, 5, 2), ShouldBeNil)
			So(testIterativeRun(3, 1000, 250), ShouldBeNil)

			// We'll use fetch/batch sizes that are not direct multiples of each other
			// so we can test some incongruent boundaries.
			So(testIterativeRun(3, 900, 250), ShouldBeNil)
		})

		Convey(`With callbacks`, func() {
			const batchSize = 16
			var countA int
			var errA error

			fds.constraints.QueryBatchSize = int(batchSize)
			c = WithQueryBatchCallback(c, func(context.Context) error {
				countA++
				return errA
			})

			q := NewQuery("")

			Convey(`Executes the callbacks during batching.`, func() {
				// Get 250% of the batch size. This will result in several full batches
				// and one partial batch, each of which should get a callback.
				limit := 2.5 * batchSize
				cbCount := int(limit / batchSize)

				q = q.Limit(int32(limit))
				var items []*CommonStruct
				So(GetAll(c, q, &items), ShouldBeNil)
				So(len(items), ShouldEqual, limit)
				So(countA, ShouldEqual, cbCount)
			})

			Convey(`Will stop querying if a callback errors.`, func() {
				errA = errors.New("test error")

				var items []*CommonStruct
				So(GetAll(c, q, &items), ShouldEqual, errA)
				So(countA, ShouldEqual, 1)
			})
		})
	})
}

func TestPutBatch(t *testing.T) {
	t.Parallel()

	Convey("A testing datastore", t, func() {
		c := info.Set(context.Background(), fakeInfo{})

		fds := fakeDatastore{}
		c = SetRawFactory(c, fds.factory())

		cf := counterFilter{}
		c = AddRawFilters(c, cf.filter())

		Convey(`Can put a single round with no callbacks.`, func(convey C) {
			fds.convey = convey
			fds.constraints.MaxPutSize = 10

			css := make([]*CommonStruct, 10)
			for i := range css {
				css[i] = &CommonStruct{Value: int64(i)}
			}

			So(Put(c, css), ShouldBeNil)
			So(cf.put, ShouldEqual, 1)
		})

		Convey(`Can put in batch.`, func(convey C) {
			fds.convey = convey
			fds.constraints.MaxPutSize = 2

			css := make([]*CommonStruct, 10)
			for i := range css {
				// 0, 1, 0, 1 since PutMulti asserts per batch numbering from 0..N.
				css[i] = &CommonStruct{Value: int64(i % 2)}
			}

			So(Put(c, css), ShouldBeNil)
			So(cf.put, ShouldEqual, 5)
		})
	})
}
