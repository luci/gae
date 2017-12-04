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

// ScopedRequestHandler offers a middleware functionality that adds
// AppEngine-like handling to individual requests. If used, a
// ScopedRequestHandler's Handle method should be called during processing of
// individual HTTP requests to add pre- and post-request processing to these
// requests.
//
// ScopedRequestHandler is not required to use the cloud implementation.
type ScopedRequestHandler struct {
	// CapturePanics, if true, instructs the ScopedRequestHandler to capture andl
	// handle any panics that occur during its request processing. IF false,
	// panics will not be caught or logged by ScopedRequestHandler.
	//
	// If a panic is caught, it will be logged to the installed logger. Handle
	// will return an error indicating that a panic was observed.
	CapturePanics bool
}

// Handle is a middleware function that wraps an HTTP request. It sets up a
// preprocessing environment, invokes the supplied Handler, and operates on the
// output after the Handler has finished.
//
// If a panic occurs during Handle, and CapturePanics is true, the panic will
// be logged and consumed, and Handle will return an error. The panic's metadata
// will be available in the error by calling ScopedRequestPanic.
//
// Handle requires a Flex GAE environment to be installed in the supplied
// Context (see Use).
func (sr *ScopedRequestHandler) Handle(c context.Context, rw http.ResponseWriter, fn HandlerFunc) (err error) {
	rs := currentRequestState(c)
	if rs == nil {
		panic("no request state is installed in the Context")
	}

	// Wrap the ResponseWriter so we can track as much of the response properties
	// as we can.
	crw := capturingResponseWriter{Inner: rw}

	// Finalize on deferred so we can handle panic scenarios.
	defer func() {
		if derr := sr.finalize(c, rs, &crw); derr != nil {
			logging.WithError(derr).Warningf(c, "Failed to finalize scoped request.")
			if err != nil {
				err = derr
			}
		}
	}()

	// If we are handling panics, handle them before we finalize.
	if sr.CapturePanics {
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

			// Note: it may be too late to send HTTP 500 if the ResponseWriter has
			// already sent headers. But there's nothing else we can do at this point
			// anyway.
			http.Error(&crw, "Internal Server Error. See logs.", http.StatusInternalServerError)

			// Record the panic as an error.
			if err == nil {
				panicTag := makeScopedRequestPanicTag(p)
				err = errors.Reason("panic caught during handler: %v", p.Reason).Tag(panicTag).Err()
			}
		})
	}

	// Invoke the actual handler.
	fn(c, &crw)
	return nil
}

func (*ScopedRequestHandler) finalize(c context.Context, rs *requestState, crw *capturingResponseWriter) error {
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

		labels := make(map[string]string, 2)
		if rs.cfg.InstanceID != "" {
			labels["appengine.googleapis.com/instance_name"] = rs.cfg.InstanceID
		}
		addTraceIDLabel(labels, rs.TraceID)

		rs.cfg.RequestLogger.Log(cloudLogging.Entry{
			Timestamp:   now,
			Severity:    rs.SeverityTracker.HighestSeverity(),
			Payload:     payload,
			Labels:      labels,
			InsertID:    rs.insertIDGenerator.Next(),
			HTTPRequest: &httpRequest,
		})
	}
	return nil
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
		crw.Status, crw.HasStatus = s, true
	}
}

////////////////////////////////////////////////////////////////////////////////
// scopedRequestPanicTag
////////////////////////////////////////////////////////////////////////////////

var scopedRequestPanicTagKey = errors.NewTagKey("the panic reason that was captured by a ScoepdRequest Handler")

type scopedRequestPanicTag struct {
	p *paniccatcher.Panic
}

func makeScopedRequestPanicTag(p *paniccatcher.Panic) errors.TagValueGenerator {
	return &scopedRequestPanicTag{p}
}

func (t *scopedRequestPanicTag) GenerateErrorTagValue() errors.TagValue {
	return errors.TagValue{Key: scopedRequestPanicTagKey, Value: t.p}
}

// ScopedRequestPanic returns the panic that a ScopedRequestHandler error hasl
// been tagged with.
//
// If the error was not triggered by a captured Panic, ScopedRequestPanic will
// return nil.
func ScopedRequestPanic(err error) *paniccatcher.Panic {
	if v, ok := errors.TagValueIn(scopedRequestPanicTagKey, err); ok {
		return v.(*paniccatcher.Panic)
	}
	return nil
}
