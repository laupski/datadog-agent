// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// Package sources provides log source configuration and management
package sources

import "slices"

// ConfigSources receives file paths to log configs and creates sources. The sources are added to a channel and read by the launcher.
// This class implements the SourceProvider interface
type ConfigSources struct {
	addedByType map[string][]*LogSource
}

// NewConfigSources provides an instance of ConfigSources.
func NewConfigSources() *ConfigSources {
	return &ConfigSources{
		addedByType: make(map[string][]*LogSource),
	}
}

// AddSource adds source to the map of stored sources by type.
func (s *ConfigSources) AddSource(source *LogSource) {
	s.addedByType[source.Config.Type] = append(s.addedByType[source.Config.Type], source)
}

// SubscribeAll is required for the SourceProvider interface
func (s *ConfigSources) SubscribeAll(_, _ chan struct{}) (added chan *LogSource, _ chan *LogSource) {
	return
}

// SubscribeForType returns a channel carrying LogSources for a given source type
func (s *ConfigSources) SubscribeForType(sourceType string, addedDone, _ chan struct{}) (added chan *LogSource, _ chan *LogSource) {
	added = make(chan *LogSource)
	go func() {
		for _, logSource := range s.addedByType[sourceType] {
			select {
			case added <- logSource:
			case <-addedDone:
				return
			}
		}
	}()

	return added, nil
}

// SubscribeForTypeBatches returns all configured sources of the requested type
// in one notification.
func (s *ConfigSources) SubscribeForTypeBatches(sourceType string, addedDone, _ chan struct{}) (chan []*LogSource, chan *LogSource) {
	added := make(chan []*LogSource)
	batch := slices.Clone(s.addedByType[sourceType])
	if len(batch) > 0 {
		go func() {
			select {
			case added <- batch:
			case <-addedDone:
			}
		}()
	}
	return added, nil
}

// GetAddedForType is required for the SourceProvider interface
func (s *ConfigSources) GetAddedForType(_ string, _ chan struct{}) chan *LogSource {
	return nil
}
