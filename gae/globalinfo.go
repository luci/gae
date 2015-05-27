// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"time"

	"golang.org/x/net/context"

	"appengine"

	"infra/gae/libs/wrapper"
)

// UseGI adds a wrapper.GlobalInfo implementation to context, accessible
// by wrapper.GetGI(c)
func UseGI(c context.Context) context.Context {
	return wrapper.SetGIFactory(c, func(ci context.Context) wrapper.GlobalInfo {
		return giImpl{ctx(c).Context, ci}
	})
}

type giImpl struct {
	appengine.Context
	ctx context.Context
}

func (g giImpl) AccessToken(scopes ...string) (token string, expiry time.Time, err error) {
	return appengine.AccessToken(g, scopes...)
}
func (g giImpl) AppID() string {
	return appengine.AppID(g)
}
func (g giImpl) Datacenter() string {
	return appengine.Datacenter()
}
func (g giImpl) DefaultVersionHostname() string {
	return appengine.DefaultVersionHostname(g)
}
func (g giImpl) InstanceID() string {
	return appengine.InstanceID()
}
func (g giImpl) IsCapabilityDisabled(err error) bool {
	return appengine.IsCapabilityDisabled(err)
}
func (g giImpl) IsDevAppserver() bool {
	return appengine.IsDevAppServer()
}
func (g giImpl) IsOverQuota(err error) bool {
	return appengine.IsOverQuota(err)
}
func (g giImpl) IsTimeoutError(err error) bool {
	return appengine.IsTimeoutError(err)
}
func (g giImpl) ModuleHostname(module, version, instance string) (string, error) {
	return appengine.ModuleHostname(g, module, version, instance)
}
func (g giImpl) ModuleName() (name string) {
	return appengine.ModuleName(g)
}
func (g giImpl) Namespace(namespace string) (context.Context, error) {
	gaeC, err := appengine.Namespace(g, namespace)
	if err != nil {
		return nil, err
	}
	return Enable(g.ctx, gaeC), nil
}
func (g giImpl) PublicCertificates() ([]appengine.Certificate, error) {
	return appengine.PublicCertificates(g)
}
func (g giImpl) RequestID() string {
	return appengine.RequestID(g)
}
func (g giImpl) ServerSoftware() string {
	return appengine.ServerSoftware()
}
func (g giImpl) ServiceAccount() (string, error) {
	return appengine.ServiceAccount(g)
}
func (g giImpl) SignBytes(bytes []byte) (keyName string, signature []byte, err error) {
	return appengine.SignBytes(g, bytes)
}
func (g giImpl) VersionID() string {
	return appengine.VersionID(g)
}
