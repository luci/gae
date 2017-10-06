// Copyright 2017 The LUCI Authors.
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

package memory

import (
	"testing"

	ds "go.chromium.org/gae/service/datastore"

	. "github.com/smartystreets/goconvey/convey"
	"go.chromium.org/luci/common/data/stringset"
)

func TestIndexSelection(t *testing.T) {
	t.Parallel()

	Convey("Test index selection", t, func() {
		store := newMemStore()
		kc := ds.MkKeyContext("appid", "")
		rq := &reducedQuery{
			kc:   kc,
			kind: "kind",
			eqFilters: map[string]stringset.Set{
				"f1": stringset.NewFromSlice("f1_1"),
			},
			suffixFormat: []ds.IndexColumn{
				{Property: "__key__"},
			},
		}
		def, err := getIndexes(rq, store)
		So(err, ShouldBeNil)
		So(def, ShouldBeNil)
	})
}
