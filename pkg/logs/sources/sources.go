// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package sources

import (
	"slices"
	"sync"

	"github.com/DataDog/datadog-agent/pkg/util/log"
)

type subscription struct {
	ch   chan *LogSource
	done chan struct{}
}

type batchSubscription struct {
	ch   chan []*LogSource
	done chan struct{}
}

// LogSources serves as the interface between Schedulers and Launchers, distributing
// notifications of added/removed LogSources to subscribed Launchers.
//
// Each subscription receives its own unbuffered channel for sources, and should
// consume from the channel quickly to avoid blocking other goroutines.
// Callers must provide a done channel and close it when they stop consuming,
// so that blocked sends can be skipped.
//
// If any sources have been added when GetAddedForType is called, then those sources
// are immediately sent to the channel.
//
// This type is threadsafe, and all of its methods can be called concurrently.
type LogSources struct {
	mu      sync.Mutex
	sources []*LogSource
	// Validation compiles processing rules, so replay must reuse its result.
	validSources       map[*LogSource]struct{}
	added              []*subscription
	addedByType        map[string][]*subscription
	addedBatchesByType map[string][]*batchSubscription
	removed            []*subscription
	removedByType      map[string][]*subscription
}

// NewLogSources creates a new log sources.
func NewLogSources() *LogSources {
	return &LogSources{
		validSources:       make(map[*LogSource]struct{}),
		addedByType:        make(map[string][]*subscription),
		addedBatchesByType: make(map[string][]*batchSubscription),
		removedByType:      make(map[string][]*subscription),
	}
}

// AddSource adds a new source.
//
// All of the subscribers registered for this source's type (src.Config.Type) will be
// notified.
func (s *LogSources) AddSource(source *LogSource) {
	s.AddSources([]*LogSource{source})
}

// AddSources registers a complete configuration before notifying subscribers.
// Individual subscriptions receive each valid source in order, while batch
// subscriptions receive all valid sources of their type in one notification.
func (s *LogSources) AddSources(sources []*LogSource) {
	for _, source := range sources {
		log.Tracef("Adding %s", source.Dump(false))
	}
	s.mu.Lock()
	s.sources = append(s.sources, sources...)
	validSources := make([]*LogSource, 0, len(sources))
	sourcesByType := make(map[string][]*LogSource)
	streamsByType := make(map[string][]*subscription)
	batchStreamsByType := make(map[string][]*batchSubscription)
	for _, source := range sources {
		if source.Config == nil || source.Config.Validate() != nil {
			continue
		}
		s.validSources[source] = struct{}{}
		validSources = append(validSources, source)
		sourceType := source.Config.Type
		sourcesByType[sourceType] = append(sourcesByType[sourceType], source)
		streamsByType[sourceType] = s.addedByType[sourceType]
		batchStreamsByType[sourceType] = s.addedBatchesByType[sourceType]
	}
	streams := s.added
	s.mu.Unlock()

	for _, source := range validSources {
		for _, stream := range streams {
			select {
			case stream.ch <- source:
			case <-stream.done:
			}
		}
		for _, stream := range streamsByType[source.Config.Type] {
			select {
			case stream.ch <- source:
			case <-stream.done:
			}
		}
	}

	for sourceType, batch := range sourcesByType {
		for _, stream := range batchStreamsByType[sourceType] {
			select {
			case stream.ch <- batch:
			case <-stream.done:
			}
		}
	}
}

// RemoveSource removes a source.
//
// All of the subscribers registered for this source's type (src.Config.Type) will be
// notified of its removal.
func (s *LogSources) RemoveSource(source *LogSource) {
	log.Tracef("Removing %s", source.Dump(false))
	s.mu.Lock()
	var sourceFound bool
	for i, src := range s.sources {
		if src == source {
			s.sources = slices.Delete(s.sources, i, i+1)
			if !slices.Contains(s.sources, source) {
				delete(s.validSources, source)
			}
			sourceFound = true
			break
		}
	}
	streams := s.removed
	streamsForType := s.removedByType[source.Config.Type]
	s.mu.Unlock()

	if sourceFound {
		for _, stream := range streams {
			select {
			case stream.ch <- source:
			case <-stream.done:
			}
		}
		for _, stream := range streamsForType {
			select {
			case stream.ch <- source:
			case <-stream.done:
			}
		}
	}
}

// SubscribeAll returns two channels carrying notifications of all added and
// removed sources, respectively.  This guarantees consistency if sources are
// added or removed concurrently.
//
// Any sources added before this call are delivered from a new goroutine.
func (s *LogSources) SubscribeAll(addedDone, removedDone chan struct{}) (chan *LogSource, chan *LogSource) {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := &subscription{ch: make(chan *LogSource), done: addedDone}
	removed := &subscription{ch: make(chan *LogSource), done: removedDone}

	s.added = append(s.added, added)
	s.removed = append(s.removed, removed)

	existingSources := slices.Clone(s.sources) // clone for goroutine
	go func() {
		for _, source := range existingSources {
			select {
			case added.ch <- source:
			case <-addedDone:
				return
			}
		}
	}()

	return added.ch, removed.ch
}

// SubscribeForType returns two channels carrying notifications of added and
// removed sources with the given type, respectively.  This guarantees
// consistency if sources are added or removed concurrently.
//
// Any sources added before this call are delivered from a new goroutine.
func (s *LogSources) SubscribeForType(sourceType string, addedDone, removedDone chan struct{}) (chan *LogSource, chan *LogSource) {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := &subscription{ch: make(chan *LogSource), done: addedDone}
	removed := &subscription{ch: make(chan *LogSource), done: removedDone}

	if _, exists := s.addedByType[sourceType]; !exists {
		s.addedByType[sourceType] = []*subscription{}
	}
	s.addedByType[sourceType] = append(s.addedByType[sourceType], added)

	if _, exists := s.removedByType[sourceType]; !exists {
		s.removedByType[sourceType] = []*subscription{}
	}
	s.removedByType[sourceType] = append(s.removedByType[sourceType], removed)

	existingSources := slices.Clone(s.sources) // clone for goroutine
	go func() {
		for _, source := range existingSources {
			if source.Config.Type == sourceType {
				select {
				case added.ch <- source:
				case <-addedDone:
					return
				}
			}
		}
	}()

	return added.ch, removed.ch
}

// SubscribeForTypeBatches subscribes to additions grouped by configuration and
// individual removals for the given source type. Previously registered valid
// sources are replayed as one batch. Subscribers must not modify batch slices.
func (s *LogSources) SubscribeForTypeBatches(sourceType string, addedDone, removedDone chan struct{}) (chan []*LogSource, chan *LogSource) {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := &batchSubscription{ch: make(chan []*LogSource), done: addedDone}
	removed := &subscription{ch: make(chan *LogSource), done: removedDone}
	s.addedBatchesByType[sourceType] = append(s.addedBatchesByType[sourceType], added)
	s.removedByType[sourceType] = append(s.removedByType[sourceType], removed)

	var existingSources []*LogSource
	for _, source := range s.sources {
		if _, valid := s.validSources[source]; valid && source.Config.Type == sourceType {
			existingSources = append(existingSources, source)
		}
	}
	if len(existingSources) > 0 {
		go func() {
			select {
			case added.ch <- existingSources:
			case <-addedDone:
			}
		}()
	}

	return added.ch, removed.ch
}

// GetAddedForType returns a channel carrying notifications of new sources
// with the given type.
//
// Any sources added before this call are delivered from a new goroutine.
func (s *LogSources) GetAddedForType(sourceType string, addedDone chan struct{}) chan *LogSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.addedByType[sourceType]
	if !exists {
		s.addedByType[sourceType] = []*subscription{}
	}

	stream := &subscription{ch: make(chan *LogSource), done: addedDone}
	s.addedByType[sourceType] = append(s.addedByType[sourceType], stream)

	existingSources := slices.Clone(s.sources) // clone for goroutine
	go func() {
		for _, source := range existingSources {
			if source.Config.Type == sourceType {
				select {
				case stream.ch <- source:
				case <-addedDone:
					return
				}
			}
		}
	}()

	return stream.ch
}

// GetSources returns all the sources currently held.  The result is copied and
// will not be modified after it is returned.  However, the copy in the LogSources
// instance may change in that time (changing indexes or adding/removing entries).
func (s *LogSources) GetSources() []*LogSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := slices.Clone(s.sources)
	return clone
}
