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
	"fmt"
	"net/http"
	"os"
	"strconv"

	"go.chromium.org/luci/common/clock"
	"go.chromium.org/luci/common/errors"
	"go.chromium.org/luci/common/logging"
	"go.chromium.org/luci/common/runtime/paniccatcher"

	cloudLogging "cloud.google.com/go/logging"

	"golang.org/x/net/context"
)

// HandlerFunc is a callback to handle the HTTP request.
type HandlerFunc func(context.Context, http.ResponseWriter)

func Handle(c context.Context, rw http.ResponseWriter, fn HandlerFunc) {
	req := currentRequest(c)
	if traceID := getCloudTraceContext(req); traceID != "" {
		// Use the trace ID in the header if available.
		c = cloudlogger.StartTraceWithID(c, req, traceID)
	} else {
		c = cloudlogger.StartTrace(c, req)
	}

	// Wrap the ResponseWriter so we can track as much of the response properties
	// as we can.
	crw := capturingResponseWriter{Inner: rw}

	// Defer the finalize call.  This handles panic scenarios.
	defer cloudlogging.EndTrace(c, &http.Response{
		StatusCode:    crw.Status,
		ContentLength: crw.Bytes,
	})

	// Invoke the actual handler.
	fn(c, &crw)
	return
}

////////////////////////////////////////////////////////////////////////////////
// capturingResponseWriter
////////////////////////////////////////////////////////////////////////////////

// capturingResponseWriter wraps an http.ResponseWriter, capturing metadata
// related to the response.
type capturingResponseWriter struct {
	// Inner is the inner ResponseWriter.
	Inner http.ResponseWriter

	// Bytes is the total number of response bytes written.
	Bytes int64

	// Status is the value of the set HTTP status. It is only valid if HasStatus
	// is true.
	Status int
}

func (crw *capturingResponseWriter) Header() http.Header { return crw.Inner.Header() }

func (crw *capturingResponseWriter) Write(p []byte) (int, error) {
	crw.maybeRecordStatus(http.StatusOK)
	amt, err := crw.Inner.Write(p)
	crw.Bytes += int64(amt)
	return amt, err
}

func (crw *capturingResponseWriter) WriteHeader(status int) {
	crw.maybeRecordStatus(status)
	crw.Inner.WriteHeader(status)
}

func (crw *capturingResponseWriter) maybeRecordStatus(s int) {
	if crw.Status != 0 {
		crw.Status = s
	}
}

// Flush implements http.Flusher by passing through the flush call if the underlying
// ResponseWriter implements it.
func (crw *capturingResponseWriter) Flush() {
	if f, ok := crw.Inner.(http.Flusher); ok {
		f.Flush()
	}
}
