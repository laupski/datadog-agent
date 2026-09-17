// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package sources

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/datadog-agent/comp/logs/agent/config"
)

func TestAddSource(t *testing.T) {
	sources := NewLogSources()
	assert.Equal(t, 0, len(sources.GetSources()))

	sources.AddSource(NewLogSource("foo", &config.LogsConfig{Type: "boo"}))
	assert.Equal(t, 1, len(sources.GetSources()))

	sources.AddSource(NewLogSource("bar", &config.LogsConfig{Type: "boo"}))
	assert.Equal(t, 2, len(sources.GetSources()))

	sources.AddSource(NewLogSource("baz", &config.LogsConfig{})) // invalid config
	assert.Equal(t, 3, len(sources.GetSources()))
}

func TestAddSourcesRegistersCompleteBatchBeforeNotifications(t *testing.T) {
	logSources := NewLogSources()
	done := make(chan struct{})
	defer close(done)
	individual, individualRemoved := logSources.SubscribeAll(done, done)
	typed, typedRemoved := logSources.SubscribeForType(config.FileType, done, done)
	batches, removed := logSources.SubscribeForTypeBatches(config.FileType, done, done)
	first := NewLogSource("first", &config.LogsConfig{Type: config.FileType, Path: "first.log"})
	second := NewLogSource("second", &config.LogsConfig{Type: config.FileType, Path: "second.log"})
	otherType := NewLogSource("other", &config.LogsConfig{Type: config.DockerType})
	invalid := NewLogSource("invalid", &config.LogsConfig{Type: config.FileType})
	batch := []*LogSource{first, otherType, invalid, second}
	complete := make(chan struct{})
	go func() {
		logSources.AddSources(batch)
		close(complete)
	}()

	assert.Same(t, first, receiveSourceNotification(t, individual))
	assert.Equal(t, batch, logSources.GetSources())
	assert.Same(t, first, receiveSourceNotification(t, typed))
	assert.Same(t, otherType, receiveSourceNotification(t, individual))
	assert.Same(t, second, receiveSourceNotification(t, individual))
	assert.Same(t, second, receiveSourceNotification(t, typed))
	assert.Equal(t, []*LogSource{first, second}, receiveSourceNotification(t, batches))
	receiveSourceNotification(t, complete)

	go logSources.RemoveSource(first)
	assert.Same(t, first, receiveSourceNotification(t, individualRemoved))
	assert.Same(t, first, receiveSourceNotification(t, typedRemoved))
	assert.Same(t, first, receiveSourceNotification(t, removed))
}

func TestSubscribeForTypeBatchesReplaysValidSourcesTogether(t *testing.T) {
	logSources := NewLogSources()
	first := NewLogSource("first", &config.LogsConfig{Type: config.FileType, Path: "first.log"})
	second := NewLogSource("second", &config.LogsConfig{Type: config.FileType, Path: "second.log"})
	logSources.AddSources([]*LogSource{
		first,
		NewLogSource("invalid", &config.LogsConfig{Type: config.FileType}),
		NewLogSource("missing config", nil),
		NewLogSource("other", &config.LogsConfig{Type: config.DockerType}),
		second,
	})
	done := make(chan struct{})
	defer close(done)
	batches, _ := logSources.SubscribeForTypeBatches(config.FileType, done, done)
	assert.Equal(t, []*LogSource{first, second}, receiveSourceNotification(t, batches))

	third := NewLogSource("third", &config.LogsConfig{Type: config.FileType, Path: "third.log"})
	go logSources.AddSource(third)
	assert.Equal(t, []*LogSource{third}, receiveSourceNotification(t, batches))
}

func TestSubscribeForTypeBatchesSkipsStoppedSubscribers(t *testing.T) {
	logSources := NewLogSources()
	oldDone := make(chan struct{})
	logSources.SubscribeForTypeBatches(config.FileType, oldDone, oldDone)
	close(oldDone)
	done := make(chan struct{})
	defer close(done)
	batches, removed := logSources.SubscribeForTypeBatches(config.FileType, done, done)
	source := NewLogSource("file", &config.LogsConfig{Type: config.FileType, Path: "file.log"})
	complete := make(chan struct{})
	go func() {
		logSources.AddSource(source)
		logSources.RemoveSource(source)
		close(complete)
	}()
	assert.Equal(t, []*LogSource{source}, receiveSourceNotification(t, batches))
	assert.Same(t, source, receiveSourceNotification(t, removed))
	receiveSourceNotification(t, complete)
}

func receiveSourceNotification[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for source notification")
		var zero T
		return zero
	}
}

func TestRemoveSource(t *testing.T) {
	sources := NewLogSources()
	source1 := NewLogSource("foo", &config.LogsConfig{Type: "boo"})
	source2 := NewLogSource("bar", &config.LogsConfig{Type: "boo"})

	sources.AddSource(source1)
	sources.AddSource(source2)
	assert.Equal(t, 2, len(sources.GetSources()))

	sources.RemoveSource(source1)
	assert.Equal(t, 1, len(sources.GetSources()))
	assert.Equal(t, source2, sources.GetSources()[0])

	sources.RemoveSource(source2)
	assert.Equal(t, 0, len(sources.GetSources()))
}

func TestGetSources(t *testing.T) {
	sources := NewLogSources()
	assert.Equal(t, 0, len(sources.GetSources()))

	sources.AddSource(NewLogSource("", &config.LogsConfig{Type: "boo"}))
	assert.Equal(t, 1, len(sources.GetSources()))
}

func TestGetAddedForType(t *testing.T) {
	sources := NewLogSources()
	source := NewLogSource("foo", &config.LogsConfig{Type: "foo"})

	stream1 := sources.GetAddedForType("foo", make(chan struct{}))
	assert.NotNil(t, stream1)

	stream2 := sources.GetAddedForType("foo", make(chan struct{}))
	assert.NotNil(t, stream2)

	go func() { sources.AddSource(source) }()
	s1 := <-stream1
	s2 := <-stream2
	assert.Equal(t, s1, source)
	assert.Equal(t, s2, source)
}

func TestGetAddedForTypeExistingSources(t *testing.T) {
	sources := NewLogSources()
	source1 := NewLogSource("one", &config.LogsConfig{Type: "foo"})
	source1bar := NewLogSource("one-bar", &config.LogsConfig{Type: "bar"})
	source2 := NewLogSource("two", &config.LogsConfig{Type: "foo"})
	source2bar := NewLogSource("two-bar", &config.LogsConfig{Type: "bar"})
	source3 := NewLogSource("three", &config.LogsConfig{Type: "foo"})

	go func() {
		sources.AddSource(source1bar)
		sources.AddSource(source1)
	}()

	streamA := sources.GetAddedForType("foo", make(chan struct{}))
	assert.NotNil(t, streamA)
	sa1 := <-streamA
	assert.Equal(t, sa1, source1)

	go func() {
		sources.AddSource(source2bar)
		sources.AddSource(source2)
	}()
	sa2 := <-streamA
	assert.Equal(t, sa2, source2)

	streamB := sources.GetAddedForType("foo", make(chan struct{}))
	assert.NotNil(t, streamB)
	sb1 := <-streamB
	sb2 := <-streamB
	assert.ElementsMatch(t, []*LogSource{source1, source2}, []*LogSource{sb1, sb2})

	go func() { sources.AddSource(source3) }()
	sa3 := <-streamA
	sb3 := <-streamB
	assert.Equal(t, sa3, source3)
	assert.Equal(t, sb3, source3)
}

func TestSubscribeAll(t *testing.T) {
	sources := NewLogSources()
	source1 := NewLogSource("one", &config.LogsConfig{Type: "foo"})
	source2 := NewLogSource("two-bar", &config.LogsConfig{Type: "bar"})
	source3 := NewLogSource("three", &config.LogsConfig{Type: "foo"})

	go func() { sources.AddSource(source1) }()

	addA, removeA := sources.SubscribeAll(make(chan struct{}), make(chan struct{}))
	assert.NotNil(t, addA)
	sa1 := <-addA
	assert.Equal(t, sa1, source1)
	assert.Equal(t, 0, len(removeA))

	go func() { sources.AddSource(source2) }()

	sa2 := <-addA
	assert.Equal(t, sa2, source2)
	assert.Equal(t, 0, len(removeA))

	addB, removeB := sources.SubscribeAll(make(chan struct{}), make(chan struct{}))
	assert.NotNil(t, addB)
	sb1 := <-addB
	sb2 := <-addB
	assert.ElementsMatch(t, []*LogSource{source1, source2}, []*LogSource{sb1, sb2})
	assert.Equal(t, 0, len(removeB))

	go func() { sources.AddSource(source3) }()

	sa3 := <-addA
	sb3 := <-addB
	assert.Equal(t, sa3, source3)
	assert.Equal(t, sb3, source3)

	assert.Equal(t, 0, len(removeA))
	assert.Equal(t, 0, len(removeB))

	go func() { sources.RemoveSource(source1) }()

	sa1 = <-removeA
	sb1 = <-removeB
	assert.Equal(t, sa1, source1)
	assert.Equal(t, sb1, source1)
}

func TestSubscribeForType(t *testing.T) {
	sources := NewLogSources()
	source1 := NewLogSource("one", &config.LogsConfig{Type: "foo"})
	source1bar := NewLogSource("one-bar", &config.LogsConfig{Type: "bar"})
	source2 := NewLogSource("two", &config.LogsConfig{Type: "foo"})
	source2bar := NewLogSource("two-bar", &config.LogsConfig{Type: "bar"})
	source3 := NewLogSource("three", &config.LogsConfig{Type: "foo"})

	go func() {
		sources.AddSource(source1bar)
		sources.AddSource(source1)
	}()

	addA, removeA := sources.SubscribeForType("foo", make(chan struct{}), make(chan struct{}))
	assert.NotNil(t, addA)
	sa1 := <-addA
	assert.Equal(t, sa1, source1)
	assert.Equal(t, 0, len(removeA))

	go func() {
		sources.AddSource(source2bar)
		sources.AddSource(source2)
	}()

	sa2 := <-addA
	assert.Equal(t, sa2, source2)

	addB, removeB := sources.SubscribeForType("foo", make(chan struct{}), make(chan struct{}))
	assert.NotNil(t, addB)
	sb1 := <-addB
	sb2 := <-addB
	assert.ElementsMatch(t, []*LogSource{source1, source2}, []*LogSource{sb1, sb2})
	assert.Equal(t, 0, len(removeB))

	go func() { sources.AddSource(source3) }()

	sa3 := <-addA
	sb3 := <-addB
	assert.Equal(t, sa3, source3)
	assert.Equal(t, sb3, source3)

	assert.Equal(t, 0, len(removeA))
	assert.Equal(t, 0, len(removeB))

	go func() { sources.RemoveSource(source1) }()

	sa1 = <-removeA
	sb1 = <-removeB
	assert.Equal(t, sa1, source1)
	assert.Equal(t, sb1, source1)
}

// TestPartialRestart verifies that AddSource does not block after a launcher
// stops (closes its done channel) while a new launcher is subscribed.
//
// This covers the partial pipeline restart scenario: LogSources is reused
// across restarts, so dead subscriptions from the old launcher must not
// block sends to new subscribers.
func TestPartialRestart(t *testing.T) {
	logSources := NewLogSources()
	source := NewLogSource("foo", &config.LogsConfig{Type: "foo"})

	// Simulate old launcher: subscribe but never consume, then stop.
	oldDone := make(chan struct{})
	_ = logSources.GetAddedForType("foo", oldDone)
	close(oldDone) // old launcher stopped; its channel is now dead

	// Simulate new launcher: subscribe and consume.
	newDone := make(chan struct{})
	newStream := logSources.GetAddedForType("foo", newDone)

	addSourceDone := make(chan struct{})
	go func() {
		logSources.AddSource(source)
		close(addSourceDone)
	}()

	select {
	case received := <-newStream:
		assert.Equal(t, source, received)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for new subscriber to receive source")
	}

	select {
	case <-addSourceDone:
		// AddSource completed without blocking on the dead subscription
	case <-time.After(time.Second):
		t.Fatal("AddSource blocked on dead subscription")
	}
}
