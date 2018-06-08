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

package cloud

import (
	"net/http"
	"time"

	"go.chromium.org/gae/impl/dummy"
	ds "go.chromium.org/gae/service/datastore"
	"go.chromium.org/gae/service/mail"
	mc "go.chromium.org/gae/service/memcache"
	"go.chromium.org/gae/service/module"
	"go.chromium.org/gae/service/taskqueue"
	"go.chromium.org/gae/service/user"

	"go.chromium.org/luci/common/clock"

	"cloud.google.com/go/datastore"
	cloudLogging "cloud.google.com/go/logging"
	"github.com/bradfitz/gomemcache/memcache"

	"golang.org/x/net/context"
)

var requestStateContextKey = "gae/flex request state"

// Config is a full-stack cloud service configuration. A user can selectively
// populate its fields, and services for the populated fields will be installed
// in the Context and available.
//
// Because the "impl/cloud" service collection is a composite set of cloud
// services, the user can choose services based on their configuration.
//
// The parameters of Config are mostly consumed by the "service/info" service
// implementation, which describes the environment in which the service is run.
type Config struct {
	// IsDev is true if this is a development execution.
	IsDev bool

	// ProjectID, if not empty, is the project ID returned by the "info" service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	ProjectID string

	// ServiceName, if not empty, is the service (module) name returned by the
	// "info" service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	ServiceName string

	// VersionName, if not empty, is the version name returned by the "info"
	// service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	VersionName string

	// InstanceID, if not empty, is the instance ID returned by the "info"
	// service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	InstanceID string

	// ServiceAccountName, if not empty, is the service account name returned by
	// the "info" service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	ServiceAccountName string

	// ServiceProvider, if not nil, is the system service provider to use for
	// non-cloud external resources and services.
	//
	// If nil, the service will treat requests for services as not implemented.
	ServiceProvider ServiceProvider

	// DS is the cloud datastore client. If populated, the datastore service will
	// be installed.
	DS *datastore.Client

	// MC is the memcache service client. If populated, the memcache service will
	// be installed.
	MC *memcache.Client

	// RequestLogger, if not nil, will be used by ScopedRequest to log
	// request-level logs.
	//
	// The request log is a per-request high-level log that shares a Trace ID
	// with individual debug logs, has request-wide metadata, and is given the
	// severity of the highest debug log emitted during the handling of the
	// request.
	RequestLogger *cloudLogging.Logger

	// DebugLogger, if not nil, will cause a Stackdriver Logging client to be
	// installed into the Context by Use for debug logging messages.
	//
	// Debug logging messages are individual application log messages emitted
	// through the "logging.Logger" interface installed by Use. All logs emitted
	// through the Logger are considered debug logs, regardless of theirl
	// individual Level.
	DebugLogger *cloudLogging.Logger
}

// currentRequest returns the *http.Request in c. If no *http.Request is
// installed, currentRequest will return nil.
func currentRequest(c context.Context) *http.Request {
	r, _ := c.Value(&requestStateContextKey).(*http.Request)
	return r
}

// withRequest installs an *http.Request into the context.
func withRequest(c context.Context, r *http.Request) context.Context {
	return context.WithValue(c, &requestKey, r)
}

// Use installs the Config into the supplied Context. Services will be installed
// based on the fields that are populated in Config.
//
// Any services that are missing will have "impl/dummy" stubs installed. These
// stubs will panic if called.
func (cfg *Config) Use(c context.Context) context.Context {
	// Dummy services that we don't support.
	c = mail.Set(c, dummy.Mail())
	c = module.Set(c, dummy.Module())
	c = taskqueue.SetRaw(c, dummy.TaskQueue())
	c = user.Set(c, dummy.User())

	// Configure a Stdout logger to run always.
	c = gologger.StdConfig.Use(c)
	// If a Cloud Logger is configured, append it to the list of loggers.
	if cfg.RequestLogger != nil {
		// cloudlogger accepts nil DebugLoggers, so we only need to check for RequestLogger.
		c = teelogger.Use(c, cloudlogger.Use(c, cfg.RequestLogger, cfg.DebugLogger))
	}

	// Setup and install the "info" service.
	gi := serviceInstanceGlobalInfo{
		Config: cfg,
	}
	c = useInfo(c, &gi)

	// datastore service
	if cfg.DS != nil {
		cds := cloudDatastore{
			client: cfg.DS,
		}
		c = cds.use(c)
	} else {
		c = ds.SetRaw(c, dummy.Datastore())
	}

	// memcache service
	if cfg.MC != nil {
		mc := memcacheClient{
			client: cfg.MC,
		}
		c = mc.use(c)
	} else {
		c = mc.SetRaw(c, dummy.Memcache())
	}

	return c
}
