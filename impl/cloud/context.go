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
	"go.chromium.org/gae/impl/dummy"
	ds "go.chromium.org/gae/service/datastore"
	"go.chromium.org/gae/service/mail"
	mc "go.chromium.org/gae/service/memcache"
	"go.chromium.org/gae/service/module"
	"go.chromium.org/gae/service/taskqueue"
	"go.chromium.org/gae/service/user"

	"cloud.google.com/go/datastore"
	cloudLogging "cloud.google.com/go/logging"
	"github.com/bradfitz/gomemcache/memcache"

	"golang.org/x/net/context"
)

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

	// L is the Cloud Logging logger to use for requests. If populated, the
	// logging service will be installed.
	L *cloudLogging.Logger
}

// Request is the set of request-specific parameters.
type Request struct {
	// TraceID, if not empty, is the request's trace ID returned by the "info"
	// service.
	//
	// If empty, the service will treat requests for this field as not
	// implemented.
	TraceID string
}

// Use installs the Config into the supplied Context. Services will be installed
// based on the fields that are populated in Config.
//
// req is optional. If not nil, its fields will be used to initialize the
// services installed into the Context.
//
// Any services that are missing will have "impl/dummy" stubs installed. These
// stubs will panic if called.
func (cfg *Config) Use(c context.Context, req *Request) context.Context {
	if req == nil {
		req = &Request{}
	}

	// Dummy services that we don't support.
	c = mail.Set(c, dummy.Mail())
	c = module.Set(c, dummy.Module())
	c = taskqueue.SetRaw(c, dummy.TaskQueue())
	c = user.Set(c, dummy.User())

	// Install the logging service, if fields are sufficiently configured.
	// If no logging service is available, fall back onto an existing logger in
	// the context (usually a console (STDERR) logger).
	//
	// trace_id label is required to magically associate the logs produced by us
	// with GAE own request logs. This also assumes Resource fields in the logger
	// are already properly set to indicate "gae_app" resource. See:
	//
	//	https://github.com/GoogleCloudPlatform/google-cloud-go/issues/720
	if cfg.L != nil {
		var labels map[string]string
		if req.TraceID != "" {
			labels = map[string]string{
				"appengine.googleapis.com/trace_id": req.TraceID,
			}
		}
		c = WithLogger(c, cfg.L, labels)
	}

	// Setup and install the "info" service.
	gi := serviceInstanceGlobalInfo{
		Config:  cfg,
		Request: req,
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
