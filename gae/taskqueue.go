// Copyright 2015 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package gae

import (
	"golang.org/x/net/context"

	"appengine"
	"appengine/taskqueue"

	"infra/gae/libs/wrapper"
)

// useTQ adds a wrapper.TaskQueue implementation to context, accessible
// by wrapper.GetTQ(c)
func useTQ(c context.Context) context.Context {
	return wrapper.SetTQFactory(c, func(ci context.Context) wrapper.TaskQueue {
		return tqImpl{ctx(ci).Context}
	})
}

type tqImpl struct {
	appengine.Context
}

//////// TQSingleReadWriter

func (t tqImpl) Add(task *taskqueue.Task, queueName string) (*taskqueue.Task, error) {
	return taskqueue.Add(t.Context, task, queueName)
}
func (t tqImpl) Delete(task *taskqueue.Task, queueName string) error {
	return taskqueue.Delete(t.Context, task, queueName)
}

//////// TQMultiReadWriter

func (t tqImpl) AddMulti(tasks []*taskqueue.Task, queueName string) ([]*taskqueue.Task, error) {
	return taskqueue.AddMulti(t.Context, tasks, queueName)
}
func (t tqImpl) DeleteMulti(tasks []*taskqueue.Task, queueName string) error {
	return taskqueue.DeleteMulti(t.Context, tasks, queueName)
}

//////// TQLeaser

func (t tqImpl) Lease(maxTasks int, queueName string, leaseTime int) ([]*taskqueue.Task, error) {
	return taskqueue.Lease(t.Context, maxTasks, queueName, leaseTime)
}
func (t tqImpl) LeaseByTag(maxTasks int, queueName string, leaseTime int, tag string) ([]*taskqueue.Task, error) {
	return taskqueue.LeaseByTag(t.Context, maxTasks, queueName, leaseTime, tag)
}
func (t tqImpl) ModifyLease(task *taskqueue.Task, queueName string, leaseTime int) error {
	return taskqueue.ModifyLease(t.Context, (*taskqueue.Task)(task), queueName, leaseTime)
}

//////// TQPurger

func (t tqImpl) Purge(queueName string) error {
	return taskqueue.Purge(t.Context, queueName)
}

//////// TQStatter

func (t tqImpl) QueueStats(queueNames []string, maxTasks int) ([]taskqueue.QueueStatistics, error) {
	return taskqueue.QueueStats(t.Context, queueNames, maxTasks)
}
