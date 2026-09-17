// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	taggerfxmock "github.com/DataDog/datadog-agent/comp/core/tagger/fx-mock"
	pipelinemock "github.com/DataDog/datadog-agent/comp/logs-library/pipeline/mock"
	"github.com/DataDog/datadog-agent/comp/logs/agent/config"
	flareController "github.com/DataDog/datadog-agent/comp/logs/agent/flare"
	auditorMock "github.com/DataDog/datadog-agent/comp/logs/auditor/mock"
	configmock "github.com/DataDog/datadog-agent/pkg/config/mock"
	"github.com/DataDog/datadog-agent/pkg/logs/sources"
	"github.com/DataDog/datadog-agent/pkg/logs/tailers"
	filetailer "github.com/DataDog/datadog-agent/pkg/logs/tailers/file"
	"github.com/DataDog/datadog-agent/pkg/logs/types"
	"github.com/DataDog/datadog-agent/pkg/logs/util/opener"
)

type startupSeek struct {
	offset int64
	whence int
}

type startupOpener struct {
	opener.FileOpener
	mu         sync.Mutex
	seeks      map[string]startupSeek
	positioned chan string
}

func (o *startupOpener) OpenLogFile(path string) (afero.File, error) {
	f, err := o.FileOpener.OpenLogFile(path)
	if err != nil {
		return nil, err
	}
	return &startupFile{File: f, opener: o, path: path}, nil
}

func (o *startupOpener) firstSeek(t *testing.T, path string) startupSeek {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	seek, ok := o.seeks[path]
	require.True(t, ok, "file %s was not opened and positioned", path)
	return seek
}

type startupFile struct {
	afero.File
	opener *startupOpener
	path   string
}

func (f *startupFile) Seek(offset int64, whence int) (int64, error) {
	f.opener.mu.Lock()
	_, exists := f.opener.seeks[f.path]
	if !exists {
		f.opener.seeks[f.path] = startupSeek{offset: offset, whence: whence}
	}
	f.opener.mu.Unlock()
	position, err := f.File.Seek(offset, whence)
	if !exists && f.opener.positioned != nil {
		f.opener.positioned <- f.path
	}
	return position, err
}

// Keep tailers idle while exercising real file setup and offset validation.
func (*startupFile) Read([]byte) (int, error) { return 0, io.EOF }

func newStartupLauncher(t *testing.T, limit int) (*Launcher, *startupOpener, *auditorMock.Registry) {
	t.Helper()
	configmock.New(t)
	fileOpener := &startupOpener{FileOpener: opener.NewFileOpener(), seeks: make(map[string]startupSeek)}
	fingerprinter := filetailer.NewFingerprinter(types.FingerprintConfig{
		FingerprintStrategy: types.FingerprintStrategyDisabled,
	}, fileOpener)
	launcher := NewLauncher(limit, time.Second, false, time.Hour, WildcardModeByModificationTime,
		flareController.NewFlareController(), taggerfxmock.SetupFakeTagger(t), fileOpener, fingerprinter)
	registry := auditorMock.NewMockRegistry()
	launcher.registry = registry
	launcher.pipelineProvider = pipelinemock.NewMockProvider()
	t.Cleanup(launcher.cleanup)
	return launcher, fileOpener, registry
}

func collectStartupScan(launcher *Launcher) fileScan {
	snapshot := slices.Clone(launcher.activeSources)
	return fileScan{
		files:      launcher.fileProvider.FilesToTail(context.Background(), false, snapshot, launcher.registry),
		sources:    snapshot,
		generation: launcher.sourceGeneration,
	}
}

func startupSource(name, path string) *sources.LogSource {
	return sources.NewLogSource(name, &config.LogsConfig{Type: config.FileType, Path: path})
}

func TestLauncherModificationTimeStartupSelectsAcrossRules(t *testing.T) {
	root := t.TempDir()
	launcher, _, _ := newStartupLauncher(t, 500)
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	var rules []*sources.LogSource
	for rule := 0; rule < 3; rule++ {
		dir := filepath.Join(root, fmt.Sprintf("rule-%d", rule))
		require.NoError(t, os.Mkdir(dir, 0700))
		for i := 0; i < 501; i++ {
			path := filepath.Join(dir, fmt.Sprintf("%04d.log", i))
			require.NoError(t, os.WriteFile(path, nil, 0600))
			modified := baseTime
			if rule > 0 {
				modified = baseTime.Add(time.Duration(10000+2*i+rule) * time.Second)
			}
			require.NoError(t, os.Chtimes(path, modified, modified))
		}
		source := startupSource(fmt.Sprintf("rule-%d", rule), filepath.Join(dir, "*.log"))
		rules = append(rules, source)
		launcher.addSource(source)
		require.Zero(t, launcher.tailers.Count(), "rules must be compared before starting tailers")
	}

	launcher.applyScan(collectStartupScan(launcher))
	require.Equal(t, 500, launcher.tailers.Count())
	counts := make(map[*sources.LogSource]int)
	for _, tailer := range launcher.tailers.All() {
		counts[tailer.Source()]++
	}
	require.Equal(t, []int{0, 250, 250}, []int{counts[rules[0]], counts[rules[1]], counts[rules[2]]})
}

func TestLauncherModificationTimeStartupTailingModes(t *testing.T) {
	for _, test := range []struct {
		name       string
		mode       string
		identifier string
		offset     string
		want       startupSeek
	}{
		{name: "default", want: startupSeek{whence: io.SeekEnd}},
		{name: "end", mode: "end", want: startupSeek{whence: io.SeekEnd}},
		{name: "beginning", mode: "beginning", want: startupSeek{whence: io.SeekStart}},
		{name: "container default", identifier: "container-id", want: startupSeek{whence: io.SeekStart}},
		{name: "resume", mode: "end", offset: "4", want: startupSeek{offset: 4, whence: io.SeekStart}},
		{name: "force beginning", mode: "forceBeginning", offset: "4", want: startupSeek{whence: io.SeekStart}},
		{name: "force end", mode: "forceEnd", offset: "4", want: startupSeek{whence: io.SeekEnd}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			launcher, fileOpener, registry := newStartupLauncher(t, 2)
			path := filepath.Join(dir, "existing.log")
			require.NoError(t, os.WriteFile(path, []byte("old\ncontent\n"), 0600))
			source := startupSource("rule", filepath.Join(dir, "*.log"))
			source.Config.TailingMode = test.mode
			source.Config.Identifier = test.identifier
			registry.SetOffset("file:"+path, test.offset)
			registry.SetTailingMode(test.mode)
			launcher.addSource(source)
			launcher.applyScan(collectStartupScan(launcher))
			require.Equal(t, test.want, fileOpener.firstSeek(t, path))

			newPath := filepath.Join(dir, "new.log")
			require.NoError(t, os.WriteFile(newPath, []byte("new\ncontent\n"), 0600))
			launcher.applyScan(collectStartupScan(launcher))
			require.Equal(t, startupSeek{whence: io.SeekStart}, fileOpener.firstSeek(t, newPath),
				"files discovered after startup must be read from the beginning")
		})
	}
}

func TestLauncherModificationTimeStartupPrioritizesExplicitFiles(t *testing.T) {
	dir := t.TempDir()
	launcher, _, _ := newStartupLauncher(t, 1)
	wildcardPath := filepath.Join(dir, "new.log")
	explicitPath := filepath.Join(dir, "old.txt")
	for _, path := range []string{wildcardPath, explicitPath} {
		require.NoError(t, os.WriteFile(path, nil, 0600))
	}
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(explicitPath, old, old))
	launcher.addSource(startupSource("wildcard", filepath.Join(dir, "*.log")))
	explicit := startupSource("explicit", explicitPath)
	launcher.addSource(explicit)
	launcher.applyScan(collectStartupScan(launcher))

	require.Equal(t, 1, launcher.tailers.Count())
	tailer, ok := launcher.tailers.Get(explicitPath)
	require.True(t, ok)
	require.Same(t, explicit, tailer.Source())
}

func TestLauncherModificationTimeScanPreservesPendingSources(t *testing.T) {
	dir := t.TempDir()
	launcher, fileOpener, _ := newStartupLauncher(t, 2)
	firstPath, secondPath := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	for _, path := range []string{firstPath, secondPath} {
		require.NoError(t, os.WriteFile(path, []byte("existing\n"), 0600))
	}
	launcher.addSource(startupSource("first", firstPath))
	firstScan := collectStartupScan(launcher)
	second := startupSource("second", secondPath)
	launcher.addSource(second)
	launcher.applyScan(firstScan)
	require.Contains(t, launcher.pendingSources, second)

	launcher.applyScan(collectStartupScan(launcher))
	require.Equal(t, startupSeek{whence: io.SeekEnd}, fileOpener.firstSeek(t, secondPath))
	require.Empty(t, launcher.pendingSources)
}

func TestLauncherModificationTimeStartupMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "later.log")
	launcher, fileOpener, _ := newStartupLauncher(t, 1)
	launcher.addSource(startupSource("later", path))
	launcher.applyScan(collectStartupScan(launcher))
	require.Zero(t, launcher.tailers.Count())
	require.Empty(t, launcher.pendingSources)

	require.NoError(t, os.WriteFile(path, []byte("created after startup\n"), 0600))
	launcher.applyScan(collectStartupScan(launcher))
	require.Equal(t, startupSeek{whence: io.SeekStart}, fileOpener.firstSeek(t, path))
}

func TestLauncherModificationTimeScanIgnoresRemovedSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "removed.log")
	launcher, _, _ := newStartupLauncher(t, 1)
	require.NoError(t, os.WriteFile(path, nil, 0600))
	source := startupSource("removed", path)
	launcher.addSource(source)
	scan := collectStartupScan(launcher)
	launcher.removeSource(source)
	launcher.applyScan(scan)

	require.Zero(t, launcher.tailers.Count())
	require.Empty(t, launcher.pendingSources)
}

func TestLauncherModificationTimeScanReplacesSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.log")
	launcher, _, _ := newStartupLauncher(t, 2)
	require.NoError(t, os.WriteFile(path, nil, 0600))
	first := startupSource("first", path)
	launcher.addSource(first)
	launcher.applyScan(collectStartupScan(launcher))
	before, ok := launcher.tailers.Get(path)
	require.True(t, ok)

	replacement := startupSource("replacement", path)
	launcher.addSource(replacement)
	launcher.applyScan(collectStartupScan(launcher))
	after, ok := launcher.tailers.Get(path)
	require.True(t, ok)
	require.Same(t, before, after)
	require.Same(t, replacement, after.Source())
	require.Same(t, first.Status(), replacement.Status())
}

type startupGatedRegistry struct {
	*auditorMock.Registry
	entered   chan struct{}
	release   chan struct{}
	unblocked chan struct{}
	once      sync.Once
	unblock   sync.Once
}

func newStartupGatedRegistry(t *testing.T, registry *auditorMock.Registry) *startupGatedRegistry {
	t.Helper()
	gate := &startupGatedRegistry{
		Registry: registry, entered: make(chan struct{}), release: make(chan struct{}), unblocked: make(chan struct{}),
	}
	t.Cleanup(gate.Release)
	return gate
}

func (r *startupGatedRegistry) KeepAlive(identifier string) {
	r.once.Do(func() {
		close(r.entered)
		<-r.release
		close(r.unblocked)
	})
	r.Registry.KeepAlive(identifier)
}

func (r *startupGatedRegistry) Release() { r.unblock.Do(func() { close(r.release) }) }

func waitStartupSignal[T any](t *testing.T, signal <-chan T) T {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for launcher progress")
		var zero T
		return zero
	}
}

func startStartupLauncher(t *testing.T, launcher *Launcher, fileOpener *startupOpener) *sources.LogSources {
	t.Helper()
	fileOpener.positioned = make(chan string, launcher.tailingLimit)
	sourceProvider := sources.NewLogSources()
	launcher.Start(sourceProvider, launcher.pipelineProvider, launcher.registry, tailers.NewTailerTracker())
	t.Cleanup(launcher.Stop)
	return sourceProvider
}

func TestLauncherModificationTimeRunSelectsCompleteBatch(t *testing.T) {
	dir := t.TempDir()
	launcher, fileOpener, _ := newStartupLauncher(t, 1)
	oldPath, newPath := filepath.Join(dir, "first.old"), filepath.Join(dir, "second.new")
	for _, path := range []string{oldPath, newPath} {
		require.NoError(t, os.WriteFile(path, nil, 0600))
	}
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(oldPath, old, old))
	sourceProvider := startStartupLauncher(t, launcher, fileOpener)
	sourceProvider.AddSources([]*sources.LogSource{
		startupSource("first", filepath.Join(dir, "*.old")),
		startupSource("second", filepath.Join(dir, "*.new")),
	})

	require.Equal(t, newPath, waitStartupSignal(t, fileOpener.positioned),
		"the first opened file must reflect the complete batch without waiting for the scan timer")
}

func TestLauncherModificationTimeRunRescansSourcesAddedDuringDiscovery(t *testing.T) {
	dir := t.TempDir()
	launcher, fileOpener, registry := newStartupLauncher(t, 2)
	firstPath, secondPath := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	for _, path := range []string{firstPath, secondPath} {
		require.NoError(t, os.WriteFile(path, []byte("existing\n"), 0600))
	}
	gate := newStartupGatedRegistry(t, registry)
	defer gate.Release()
	launcher.registry = gate
	sourceProvider := startStartupLauncher(t, launcher, fileOpener)
	sourceProvider.AddSource(startupSource("first", firstPath))
	waitStartupSignal(t, gate.entered)
	sourceProvider.AddSource(startupSource("second", secondPath))
	gate.Release()

	require.Equal(t, firstPath, waitStartupSignal(t, fileOpener.positioned))
	require.Equal(t, secondPath, waitStartupSignal(t, fileOpener.positioned),
		"a source added during discovery must trigger another scan without waiting for the timer")
	require.Equal(t, startupSeek{whence: io.SeekEnd}, fileOpener.firstSeek(t, secondPath))
}

func TestLauncherModificationTimeRunStopsDuringDiscovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocked.log")
	launcher, fileOpener, registry := newStartupLauncher(t, 1)
	require.NoError(t, os.WriteFile(path, nil, 0600))
	gate := newStartupGatedRegistry(t, registry)
	defer gate.Release()
	launcher.registry = gate
	sourceProvider := startStartupLauncher(t, launcher, fileOpener)
	sourceProvider.AddSource(startupSource("blocked", path))
	waitStartupSignal(t, gate.entered)
	stopped := make(chan struct{})
	go func() {
		launcher.Stop()
		close(stopped)
	}()
	waitStartupSignal(t, stopped)
	gate.Release()
	waitStartupSignal(t, gate.unblocked)

	fileOpener.mu.Lock()
	defer fileOpener.mu.Unlock()
	require.Empty(t, fileOpener.seeks, "a scan completing after shutdown must not start tailers")
}
