// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"golang.org/x/net/context"

	"appengine"

	"github.com/mjibson/goon"
)

// Enable adds the appengine Context to c.
func Enable(c context.Context, gaeCtx appengine.Context) context.Context {
	return context.WithValue(c, goonContextKey, goon.FromContext(gaeCtx))
}

// Use calls ALL of this packages Use* methods on c. This enables all
// gae/wrapper Get* api's.
func Use(c context.Context) context.Context {
	return UseDS(UseMC(UseTQ(UseGI(c))))
}

type key int

var goonContextKey key

func ctx(c context.Context) *goon.Goon {
	return c.Value(goonContextKey).(*goon.Goon)
}
