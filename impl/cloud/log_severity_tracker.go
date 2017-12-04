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
	"sync"

	"cloud.google.com/go/logging"

	"golang.org/x/net/context"
)

var logSeverityTrackerContextKey = "gae/flex LogSeverityTracker"

// LogSeverityTracker tracks the highest observed log severity. It is safe for
// concurrent access.
type LogSeverityTracker struct {
	observed        bool
	highestSeverity logging.Severity
	lock            sync.Mutex
}

// HighestSeverity returns the highest logging severity that is been observed.
//
// If no logging severity has been explicitly observed, Default severity will
// be returned.
func (lst *LogSeverityTracker) HighestSeverity() logging.Severity {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if !lst.observed {
		return logging.Default
	}
	return lst.highestSeverity
}

// Observe updates the LogSeverityTracker's highest observed log severity.
//
// Observe is safe for concurrent usage.
func (lst *LogSeverityTracker) Observe(s logging.Severity) {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if !lst.observed || s > lst.highestSeverity {
		lst.highestSeverity = s
		lst.observed = true
	}
}

func withLogSeverityTracker(c context.Context, lst *LogSeverityTracker) context.Context {
	return context.WithValue(c, &logSeverityTrackerContextKey, lst)
}

func currentLogSeverityTracker(c context.Context) *LogSeverityTracker {
	lst, _ := c.Value(&logSeverityTrackerContextKey).(*LogSeverityTracker)
	return lst
}
