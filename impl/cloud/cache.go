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

package cloud

import (
	"time"

	"golang.org/x/net/context"
)

// ProcessCache describes a generic expiring key/value cache.
//
// Cache must be safe for concurrent access.
type ProcessCache interface {
	// Get retrieves an item from the cache. If the item isn't in the cache, or if
	// it has expired, Get will return nil.
	Get(c context.Context, key interface{}) interface{}

	// Put adds an item into the cache, overwriting any existing item.
	//
	// If exp is <= 0, the item will not have an expiration time associated with
	// it.
	Put(c context.Context, key, value interface{}, exp time.Duration)
}

type nopCache struct{}

func (nopCache) Get(context.Context, interface{}) interface{}             { return nil }
func (nopCache) Put(_ context.Context, _, _ interface{}, _ time.Duration) {}
