// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"golang.org/x/net/context"

	"appengine/datastore"

	"github.com/mjibson/goon"

	"infra/gae/libs/wrapper"
)

// UseDS adds a wrapper.Datastore implementation to context, accessible
// by wrapper.GetDS(c)
func UseDS(c context.Context) context.Context {
	return wrapper.SetDSFactory(c, func(ci context.Context) wrapper.Datastore {
		return &dsImpl{ctx(ci), ci}
	})
}

////////// Query

type queryImpl struct{ *datastore.Query }

func (q queryImpl) Distinct() wrapper.DSQuery {
	return queryImpl{q.Query.Distinct()}
}
func (q queryImpl) End(c wrapper.DSCursor) wrapper.DSQuery {
	return queryImpl{q.Query.End(c.(datastore.Cursor))}
}
func (q queryImpl) EventualConsistency() wrapper.DSQuery {
	return queryImpl{q.Query.EventualConsistency()}
}
func (q queryImpl) KeysOnly() wrapper.DSQuery {
	return queryImpl{q.Query.KeysOnly()}
}
func (q queryImpl) Limit(limit int) wrapper.DSQuery {
	return queryImpl{q.Query.Limit(limit)}
}
func (q queryImpl) Offset(offset int) wrapper.DSQuery {
	return queryImpl{q.Query.Offset(offset)}
}
func (q queryImpl) Order(fieldName string) wrapper.DSQuery {
	return queryImpl{q.Query.Order(fieldName)}
}
func (q queryImpl) Start(c wrapper.DSCursor) wrapper.DSQuery {
	return queryImpl{q.Query.Start(c.(datastore.Cursor))}
}
func (q queryImpl) Ancestor(ancestor *datastore.Key) wrapper.DSQuery {
	return queryImpl{q.Query.Ancestor(ancestor)}
}
func (q queryImpl) Project(fieldNames ...string) wrapper.DSQuery {
	return queryImpl{q.Query.Project(fieldNames...)}
}
func (q queryImpl) Filter(filterStr string, value interface{}) wrapper.DSQuery {
	return queryImpl{q.Query.Filter(filterStr, value)}
}

////////// Iterator

type iteratorImpl struct {
	*goon.Iterator
}

func (i iteratorImpl) Cursor() (wrapper.DSCursor, error) {
	return i.Iterator.Cursor()
}

////////// Datastore

type dsImpl struct {
	*goon.Goon
	c context.Context
}

// Kinder
func (g *dsImpl) KindNameResolver() goon.KindNameResolver {
	return g.Goon.KindNameResolver
}
func (g *dsImpl) SetKindNameResolver(knr goon.KindNameResolver) {
	g.Goon.KindNameResolver = knr
}

// NewKeyer
func (g *dsImpl) NewKey(kind, stringID string, intID int64, parent *datastore.Key) *datastore.Key {
	return datastore.NewKey(g.Goon.Context, kind, stringID, intID, parent)
}
func (g *dsImpl) NewKeyObj(obj interface{}) *datastore.Key {
	return g.Key(obj)
}
func (g *dsImpl) NewKeyObjError(obj interface{}) (*datastore.Key, error) {
	return g.KeyError(obj)
}

// DSQueryer
func (g *dsImpl) NewQuery(kind string) wrapper.DSQuery {
	return queryImpl{datastore.NewQuery(kind)}
}
func (g *dsImpl) Run(q wrapper.DSQuery) wrapper.DSIterator {
	return iteratorImpl{g.Goon.Run(q.(queryImpl).Query)}
}
func (g *dsImpl) Count(q wrapper.DSQuery) (int, error) {
	return g.Goon.Count(q.(queryImpl).Query)
}
func (g *dsImpl) GetAll(q wrapper.DSQuery, dst interface{}) ([]*datastore.Key, error) {
	return g.Goon.GetAll(q.(queryImpl).Query, dst)
}

// Transactioner
func (g *dsImpl) RunInTransaction(f func(c context.Context) error, opts *datastore.TransactionOptions) error {
	return g.Goon.RunInTransaction(func(ig *goon.Goon) error {
		return f(context.WithValue(g.c, goonContextKey, ig))
	}, opts)
}
