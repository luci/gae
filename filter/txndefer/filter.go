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

// Package txndefer implements a filter that calls best-effort callbacks on
// successful transaction commits.
//
// Useful when an activity inside a transaction has some best-effort follow up
// that should be done once the transaction has successfully landed.
package txndefer

import (
	"context"
	"sync"

	ds "go.chromium.org/gae/service/datastore"
)

// FilterRDS installs the datastore filter into the context.
func FilterRDS(ctx context.Context) context.Context {
	return ds.AddRawFilters(ctx, func(_ context.Context, inner ds.RawInterface) ds.RawInterface {
		return filteredDS{inner}
	})
}

// Defer schedules `cb` for execution when the current transaction successfully
// lands.
//
// Callbacks are executed sequentially in the reverse order they were deferred.
// They receive the non-transactional version of the context initially passed to
// RunInTransaction.
//
// Panics if the given context is not transactional or there's no txndefer
// filter installed.
func Defer(ctx context.Context, cb func(context.Context)) {
	state, _ := ctx.Value(&ctxKey).(*txnState)
	if state == nil {
		panic("not a transactional context or no txndefer filter installed")
	}
	state.deferCB(cb)
}

////////////////////////////////////////////////////////////////////////////////

var ctxKey = "txnfin.txnState"

type txnState struct {
	m   sync.Mutex
	cbs []func(context.Context)
}

func (s *txnState) reset() {
	s.m.Lock()
	s.cbs = s.cbs[:0]
	s.m.Unlock()
}

func (s *txnState) deferCB(cb func(context.Context)) {
	s.m.Lock()
	s.cbs = append(s.cbs, cb)
	s.m.Unlock()
}

func (s *txnState) execCBs(ctx context.Context) {
	s.m.Lock()
	defer s.m.Unlock()
	for i := len(s.cbs) - 1; i >= 0; i-- {
		s.cbs[i](ctx)
	}
}

type filteredDS struct {
	ds.RawInterface
}

func (fds filteredDS) RunInTransaction(f func(ctx context.Context) error, opts *ds.TransactionOptions) error {
	var noTxnCtx context.Context
	state := txnState{}
	err := fds.RawInterface.RunInTransaction(func(ctx context.Context) error {
		noTxnCtx = ds.WithoutTransaction(ctx)
		state.reset()
		return f(context.WithValue(ctx, &ctxKey, &state))
	}, opts)
	if err == nil {
		state.execCBs(noTxnCtx)
	}
	return err
}
