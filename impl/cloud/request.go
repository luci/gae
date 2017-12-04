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
	"go.chromium.org/luci/common/logging"
	"go.chromium.org/luci/common/runtime/paniccatcher"

	cloudLogging "cloud.google.com/go/logging"

	"golang.org/x/net/context"
)

// HandlerFunc is a callback to handle the HTTP request.
type HandlerFunc func(context.Context, http.ResponseWriter)

// WithScopedRequest is a middleware function that wraps an HTTP requerst. At
// completion, WithScopedRequest will finalize the request.
//
// WithScopedRequest is optional.
//
// WithScopedRequest will capture panics that occur during the handler function.
// If a panic is captured, WithScopedRequest will panic, but (because of panic
// capture/release), the stack trace will be re-rooted in WithScopedRequest.
// If a logger is installed, the original panic and its stack trace will be
// logged.
//
// WithScopedRequest requires a Flex GAE application to be installed in the
// supplied Context (see Use).
func WithScopedRequest(c context.Context, rw http.ResponseWriter, fn HandlerFunc) {
	rs := currentRequestState(c)
	if rs == nil {
		panic("no request state is installed in the Context")
	}

	// Wrap the ResponseWriter so we can track as much of the response properties
	// as we can.
	crw := capturingResponseWriter{Inner: rw}

	// Finalize on deferred so we can handle panic scenarios.
	defer func() {
		if err := finalizeScopedRequest(c, rs, &crw); err != nil {
			logging.WithError(err).Warningf(c, "Failed to finalize scoped request.")
		}
	}()

	// If we are handling panics, handle them before we finalize.
	if rs.cfg.HandlePanics {
		defer paniccatcher.Catch(func(p *paniccatcher.Panic) {
			uri := "UNKNOWN"
			if rs.HTTPRequest != nil {
				uri = rs.HTTPRequest.RequestURI
			}

			// Log the panic to STDERR (flex).
			fmt.Fprintf(os.Stderr, "Caught panic during handling of %q: %s\n%s\n", uri, p.Reason, p.Stack)

			// Log the reason before the stack in case the stack gets truncated
			// due to size limitations.
			logging.Fields{
				"panic.error": p.Reason,
			}.Errorf(c, "Caught panic during handling of %q: %s\n%s", uri, p.Reason, p.Stack)

			// Escalate observed log level to Critical.
			rs.SeverityTracker.Observe(cloudLogging.Critical)

			// Note: it may be too late to send HTTP 500 if `next` already sent
			// headers. But there's nothing else we can do at this point anyway.
			http.Error(&crw, "Internal Server Error. See logs.", http.StatusInternalServerError)
		})
	}

	// Invoke the actual handler.
	fn(c, &crw)
}

func finalizeScopedRequest(c context.Context, rs *requestState, crw *capturingResponseWriter) error {
	// If a request log name is supplied, emit a request logging entry.
	if rs.cfg.RequestLogger != nil {
		now := clock.Now(c)
		latency := now.Sub(rs.StartTime)

		// Build a logging HTTPRequest.
		httpRequest := cloudLogging.HTTPRequest{
			Request:      rs.HTTPRequest,
			Status:       crw.Status,
			ResponseSize: crw.Bytes,
			Latency:      latency,
			LocalIP:      rs.LocalAddr,
		}
		if req := rs.HTTPRequest; req != nil {
			httpRequest.RemoteIP = req.RemoteAddr
		}

		// The payload is copied from observed "nginx" payload logging in Flex
		// enviornment.
		payload := struct {
			LatencySeconds string `json:"latencySeconds,omitempty"`
			Trace          string `json:"trace,omitempty"`
		}{
			LatencySeconds: strconv.FormatFloat(latency.Seconds(), 'f', 3, 64),
			Trace:          rs.TraceID,
		}

		labels := map[string]string{}
		if rs.cfg.InstanceID != "" {
			labels["appengine.googleapis.com/instance_name"] = rs.cfg.InstanceID
		}
		addTraceIDLabel(labels, rs.TraceID)

		rs.cfg.RequestLogger.Log(cloudLogging.Entry{
			Timestamp: now,
			Severity:  rs.SeverityTracker.HighestSeverity(),

			// Payload is copied from observed "nginx" payload logging in Flex
			// enviornment.
			Payload:     payload,
			Labels:      labels,
			InsertID:    rs.insertIDGenerator.Next(),
			HTTPRequest: &httpRequest,
		})
	}
	return nil
}

// capturingResponseWriter is an http.ResponseWriter that collects and captures
// response metadata.
//
// capturingResponseWriter is not safe for concurrent use.
type capturingResponseWriter struct {
	// Inner is the inner ResponseWriter.
	Inner http.ResponseWriter

	// Bytes is the total number of response bytes written.
	Bytes int64

	// HasStatus is true if an HTTP status has been formally set.
	HasStatus bool
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
	if !crw.HasStatus {
		crw.Status = s
	}
}
