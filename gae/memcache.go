// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"golang.org/x/net/context"

	"appengine"
	"appengine/memcache"

	"infra/gae/libs/wrapper"
)

// UseMC adds a wrapper.Memcache implementation to context, accessible
// by wrapper.GetMC(c)
func UseMC(c context.Context) context.Context {
	return wrapper.SetMCFactory(c, func(ci context.Context) wrapper.Memcache {
		return mcImpl{ctx(c).Context}
	})
}

type mcImpl struct {
	appengine.Context
}

//////// MCSingleReadWriter
func (m mcImpl) Add(item *memcache.Item) error {
	return memcache.Add(m.Context, item)
}
func (m mcImpl) Set(item *memcache.Item) error {
	return memcache.Set(m.Context, item)
}
func (m mcImpl) Delete(key string) error {
	return memcache.Delete(m.Context, key)
}
func (m mcImpl) Get(key string) (*memcache.Item, error) {
	return memcache.Get(m.Context, key)
}
func (m mcImpl) CompareAndSwap(item *memcache.Item) error {
	return memcache.CompareAndSwap(m.Context, item)
}

//////// MCMultiReadWriter
func (m mcImpl) DeleteMulti(keys []string) error {
	return memcache.DeleteMulti(m.Context, keys)
}
func (m mcImpl) AddMulti(items []*memcache.Item) error {
	return memcache.AddMulti(m.Context, items)
}
func (m mcImpl) SetMulti(items []*memcache.Item) error {
	return memcache.SetMulti(m.Context, items)
}
func (m mcImpl) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	return memcache.GetMulti(m.Context, keys)
}
func (m mcImpl) CompareAndSwapMulti(items []*memcache.Item) error {
	return memcache.CompareAndSwapMulti(m.Context, items)
}

//////// MCIncrementer
func (m mcImpl) Increment(key string, delta int64, initialValue uint64) (uint64, error) {
	return memcache.Increment(m.Context, key, delta, initialValue)
}
func (m mcImpl) IncrementExisting(key string, delta int64) (uint64, error) {
	return memcache.IncrementExisting(m.Context, key, delta)
}

//////// MCFlusher
func (m mcImpl) Flush() error {
	return memcache.Flush(m.Context)
}

//////// MCStatter
func (m mcImpl) Stats() (*memcache.Statistics, error) {
	return memcache.Stats(m.Context)
}

//////// MCCodecInflater
type mcCodecCombiner struct {
	appengine.Context
	codec memcache.Codec
}

//////// MCCodecInflater.logger
func (cc mcCodecCombiner) Set(item *memcache.Item) error {
	return cc.codec.Set(cc.Context, item)
}
func (cc mcCodecCombiner) Add(item *memcache.Item) error {
	return cc.codec.Add(cc.Context, item)
}
func (cc mcCodecCombiner) Get(key string, v interface{}) (*memcache.Item, error) {
	return cc.codec.Get(cc.Context, key, v)
}
func (cc mcCodecCombiner) SetMulti(items []*memcache.Item) error {
	return cc.codec.SetMulti(cc.Context, items)
}
func (cc mcCodecCombiner) AddMulti(items []*memcache.Item) error {
	return cc.codec.AddMulti(cc.Context, items)
}
func (cc mcCodecCombiner) CompareAndSwap(item *memcache.Item) error {
	return cc.codec.CompareAndSwap(cc.Context, item)
}
func (cc mcCodecCombiner) CompareAndSwapMulti(items []*memcache.Item) error {
	return cc.codec.CompareAndSwapMulti(cc.Context, items)
}

func (m mcImpl) InflateCodec(codec memcache.Codec) wrapper.MCCodec {
	return mcCodecCombiner{m.Context, codec}
}
