// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"errors"
	"golang.org/x/net/context"

	"appengine"

	"github.com/mjibson/goon"
)

// Use adds implementations for the following gae/wrapper interfaces to the
// context:
//   * wrapper.Datastore
//   * wrapper.TaskQueue
//   * wrapper.Memcache
//   * wrapper.GlobalInfo
//
// These can be retrieved with the "gae/wrapper".Get functions.
//
// The implementations are all backed by the real "appengine" SDK functionality,
// and by "github.com/mjibson/goon".
//
// Using this more than once per context.Context will cause a panic.
func Use(c context.Context, gaeCtx appengine.Context) context.Context {
	if c.Value(goonContextKey) != nil {
		panic(errors.New("gae.Use: called twice on the same Context"))
	}
	c = context.WithValue(c, goonContextKey, goon.FromContext(gaeCtx))
	return useDS(useMC(useTQ(useGI(c))))
}

type key int

var goonContextKey key

func ctx(c context.Context) *goon.Goon {
	return c.Value(goonContextKey).(*goon.Goon)
}
