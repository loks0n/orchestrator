// Package docker implements the job.Orchestrator interface using the Docker API.
// Jobs run directly on the host Docker daemon.
package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"orchestrator/internal/apperrors"
	"orchestrator/internal/dispatcher"
	"orchestrator/internal/job"
	"orchestrator/internal/observability"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
)

// Orchestrator implements job.Orchestrator using Docker.
type Orchestrator struct {
	client           *client.Client
	sidecarImage     string
	retentionPeriod  time.Duration
	dispatcher       dispatcher.Dispatcher
	callbackProxyURL string
	metrics          *observability.Metrics
	state            *stateRepo

	cancelMaintenance context.CancelFunc
	watchWg           sync.WaitGroup
}

// Config holds configuration for the Docker orchestrator.
type Config struct {
	SidecarImage        string
	RetentionPeriod     time.Duration          // How long to keep completed jobs (default 15m)
	MaintenanceInterval time.Duration          // How often to run cleanup (default 1m)
	Dispatcher          dispatcher.Dispatcher  // Callback dispatcher (required)
	CallbackProxyURL    string                 // Internal URL for sidecar callbacks (e.g., http://host.docker.internal:8080)
	Metrics             *observability.Metrics // Metrics recorder (optional)
}

// NewOrchestrator creates a new Docker orchestrator.
// It automatically reconciles any jobs that were running before a restart.
func NewOrchestrator(ctx context.Context, cfg Config) (*Orchestrator, error) {
	if cfg.Dispatcher == nil {
		return nil, fmt.Errorf("dispatcher is required")
	}

	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	retentionPeriod := cfg.RetentionPeriod
	if retentionPeriod <= 0 {
		retentionPeriod = 15 * time.Minute
	}

	maintenanceInterval := cfg.MaintenanceInterval
	if maintenanceInterval <= 0 {
		maintenanceInterval = 1 * time.Minute
	}

	o := &Orchestrator{
		client:           dockerClient,
		sidecarImage:     cfg.SidecarImage,
		retentionPeriod:  retentionPeriod,
		dispatcher:       cfg.Dispatcher,
		callbackProxyURL: cfg.CallbackProxyURL,
		metrics:          cfg.Metrics,
		state:            newStateRepo(),
	}

	if err := o.reconcile(ctx); err != nil {
		slog.Warn("Failed to reconcile jobs", "error", err)
	}

	// Start background maintenance
	maintenanceCtx, cancel := context.WithCancel(context.Background())
	o.cancelMaintenance = cancel
	go o.runMaintenance(maintenanceCtx, maintenanceInterval)

	return o, nil
}

// reconcile scans Docker for existing job containers and resumes watching them.
func (o *Orchestrator) reconcile(ctx context.Context) error {
	logger := slog.With("component", "reconcile")

	// Find all containers managed by this service
	containers, err := o.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", "managed-by=jobs-service"),
		),
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	// Group containers by job ID
	type jobContainers struct {
		worker  *container.Summary
		sidecar *container.Summary
	}
	jobs := make(map[string]*jobContainers)

	for i := range containers {
		c := &containers[i]
		jobID := c.Labels["job.id"]
		if jobID == "" {
			continue
		}

		if jobs[jobID] == nil {
			jobs[jobID] = &jobContainers{}
		}

		switch c.Labels["job.type"] {
		case "worker":
			jobs[jobID].worker = c
		case "sidecar":
			jobs[jobID].sidecar = c
		}
	}

	// Rebuild state for each job
	var reconciled, running, completed int
	for jobID, jc := range jobs {
		if jc.worker == nil {
			logger.Warn("Job missing worker container", "jobId", jobID)
			continue
		}

		js := &jobState{
			jobContainerID: jc.worker.ID,
			volumeName:     fmt.Sprintf("job-%s-workspace", jobID),
		}

		if jc.sidecar != nil {
			js.sidecarContainerID = jc.sidecar.ID
		}

		o.state.commit(jobID, js)

		reconciled++

		// If worker is still running, resume watching
		if jc.worker.State == "running" {
			running++
			// Extract callback config from sidecar environment
			callbackCfg := o.extractCallbackConfig(ctx, jc.sidecar)

			watchCtx, cancelWatch := context.WithCancel(context.Background())
			js.cancelWatch = cancelWatch
			o.watchWg.Add(1)
			go func() {
				defer o.watchWg.Done()
				o.resumeWatch(watchCtx, jobID, js, callbackCfg)
			}()
		} else {
			completed++
		}
	}

	logger.Info("Reconciliation complete", "reconciled", reconciled, "running", running, "completed", completed)
	return nil
}

// callbackConfig holds callback configuration extracted from a container.
type callbackConfig struct {
	meta   map[string]string
	url    string
	key    string
	events []string
}

// callbackDest holds destination info for dispatching events.
type callbackDest struct {
	jobID  string
	meta   map[string]string
	url    string
	key    string
	events []string
}

// extractCallbackConfig extracts callback configuration from a sidecar container's environment.
func (o *Orchestrator) extractCallbackConfig(ctx context.Context, sidecar *container.Summary) *callbackConfig {
	if sidecar == nil {
		return nil
	}

	inspect, err := o.client.ContainerInspect(ctx, sidecar.ID)
	if err != nil {
		return nil
	}

	cfg := &callbackConfig{}
	for _, env := range inspect.Config.Env {
		key, value, ok := strings.Cut(env, "=")
		if !ok {
			continue
		}
		switch key {
		case "CALLBACK_URL":
			cfg.url = value
		case "CALLBACK_KEY":
			cfg.key = value
		case "CALLBACK_EVENTS":
			if value != "" {
				cfg.events = strings.Split(value, ",")
			}
		case "JOB_META":
			if value != "" {
				_ = json.Unmarshal([]byte(value), &cfg.meta)
			}
		}
	}

	if cfg.url == "" {
		return nil
	}
	return cfg
}

// resumeWatch watches a recovered job without sending the start event.
// Metrics are not recorded for resumed jobs since they weren't counted as "created" by this instance.
func (o *Orchestrator) resumeWatch(ctx context.Context, jobID string, js *jobState, callbackCfg *callbackConfig) {
	logger := slog.With("jobId", jobID, "resumed", true)

	// Build callback destination from config
	var dest *callbackDest
	if callbackCfg != nil && callbackCfg.url != "" {
		dest = &callbackDest{
			jobID:  jobID,
			meta:   callbackCfg.meta,
			url:    callbackCfg.url,
			key:    callbackCfg.key,
			events: callbackCfg.events,
		}
	}

	// Pass zero time to skip metrics for resumed jobs
	o.watchUntilExit(ctx, logger, jobID, "", js.jobContainerID, dest, time.Time{})
}

// Run creates and starts a job with its sidecar.
func (o *Orchestrator) Run(ctx context.Context, req *job.Request) error {
	if err := o.state.reserve(req.ID); err != nil {
		return err
	}

	js := &jobState{
		volumeName: fmt.Sprintf("job-%s-workspace", req.ID),
	}

	// On failure, clean up resources and release reservation
	success := false
	defer func() {
		if !success {
			o.cleanup(ctx, js)
			o.state.release(req.ID)
		}
	}()

	// Create shared volume
	if _, err := o.client.VolumeCreate(ctx, volume.CreateOptions{Name: js.volumeName}); err != nil {
		return apperrors.Internal("docker.createVolume", err)
	}

	// Pull job image if needed
	if err := o.pullImageIfNeeded(ctx, req.Image); err != nil {
		return apperrors.Internal("docker.pullImage", err)
	}

	// Create job container
	var err error
	if js.jobContainerID, err = o.createJobContainer(ctx, req, js); err != nil {
		return apperrors.Internal("docker.createJobContainer", err)
	}

	// Create sidecar container
	if js.sidecarContainerID, err = o.createSidecarContainer(ctx, req, js); err != nil {
		return apperrors.Internal("docker.createSidecarContainer", err)
	}

	// Start sidecar first (handles inputs)
	if err := o.client.ContainerStart(ctx, js.sidecarContainerID, container.StartOptions{}); err != nil {
		return apperrors.Internal("docker.startSidecarContainer", err)
	}

	// Start job container
	if err := o.client.ContainerStart(ctx, js.jobContainerID, container.StartOptions{}); err != nil {
		return apperrors.Internal("docker.startJobContainer", err)
	}

	// Commit the job state
	o.state.commit(req.ID, js)
	success = true

	// Start watching (log streaming + callbacks) in background
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	js.cancelWatch = cancelWatch
	o.watchWg.Add(1)
	go func() {
		defer o.watchWg.Done()
		o.watchJob(watchCtx, req, js)
	}()

	return nil
}

// Stop stops a running job and cleans up its resources.
func (o *Orchestrator) Stop(ctx context.Context, jobID string) error {
	js, exists := o.state.release(jobID)
	if !exists {
		return apperrors.NotFound("job", jobID)
	}

	// Job is reserved but still initializing - nothing to clean up yet
	if js == nil {
		return nil
	}

	if js.cancelWatch != nil {
		js.cancelWatch()
	}

	o.cleanup(ctx, js)
	return nil
}

// Status returns the current status of a job.
func (o *Orchestrator) Status(ctx context.Context, jobID string) (*job.Status, error) {
	js, exists := o.state.get(jobID)
	if !exists {
		return nil, apperrors.NotFound("job", jobID)
	}

	// Job is reserved but still initializing
	if js == nil {
		return &job.Status{ID: jobID, State: job.StateAccepted}, nil
	}

	inspect, err := o.client.ContainerInspect(ctx, js.jobContainerID)
	if err != nil {
		return nil, apperrors.Internal("docker.inspectContainer", err)
	}

	status := &job.Status{ID: jobID}

	if inspect.State.Running {
		status.State = job.StateRunning
		return status, nil
	}

	// Container has exited - set exit code and determine state
	exitCode := inspect.State.ExitCode
	status.ExitCode = &exitCode

	if exitCode == 0 {
		status.State = job.StateCompleted
	} else {
		status.State = job.StateFailed
		if inspect.State.Error != "" {
			status.Error = inspect.State.Error
		}
	}

	return status, nil
}

// List returns the status of all jobs.
func (o *Orchestrator) List(ctx context.Context) ([]job.Status, error) {
	jobIDs := o.state.ids()
	statuses := make([]job.Status, 0, len(jobIDs))
	for _, id := range jobIDs {
		status, err := o.Status(ctx, id)
		if err != nil {
			continue
		}
		statuses = append(statuses, *status)
	}
	return statuses, nil
}

// Close releases resources held by the orchestrator.
func (o *Orchestrator) Close() error {
	if o.cancelMaintenance != nil {
		o.cancelMaintenance()
	}

	// Cancel all watch goroutines and wait for them to finish
	for _, js := range o.state.list() {
		if js != nil && js.cancelWatch != nil {
			js.cancelWatch()
		}
	}
	o.watchWg.Wait()

	return o.client.Close()
}

// Ready checks if the Docker daemon is reachable and responsive.
func (o *Orchestrator) Ready(ctx context.Context) error {
	_, err := o.client.Ping(ctx)
	return err
}

// watchJob handles log streaming and callbacks for a job.
func (o *Orchestrator) watchJob(ctx context.Context, req *job.Request, js *jobState) {
	logger := slog.With("jobId", req.ID)
	startTime := time.Now()

	// Build callback destination
	var dest *callbackDest
	if req.Callback != nil && req.Callback.URL != "" {
		dest = &callbackDest{
			jobID:  req.ID,
			meta:   req.Meta,
			url:    req.Callback.URL,
			key:    req.Callback.Key,
			events: req.Callback.Events,
		}
	}

	// Send start event
	if dest != nil {
		builder := job.NewEventBuilder(req.ID, "orchestrator/service", dest.meta)
		event := builder.BuildStartEvent()
		if job.FilteredEvents(event.Type, dest.events) {
			if err := o.dispatcher.Dispatch(&dispatcher.Event{
				Payload:     event,
				Destination: dest.url,
				SigningKey:  dest.key,
			}); err != nil {
				logger.Warn("Failed to dispatch start event", "error", err)
			}
		}
	}

	o.watchUntilExit(ctx, logger, req.ID, req.Image, js.jobContainerID, dest, startTime)
}

// watchUntilExit streams logs and waits for a container to exit, then sends the exit callback.
func (o *Orchestrator) watchUntilExit(ctx context.Context, logger *slog.Logger, jobID, image, containerID string, dest *callbackDest, startTime time.Time) {
	// Start log streaming in background
	logCtx, logCancel := context.WithCancel(ctx)
	logDone := make(chan struct{})
	go func() {
		defer close(logDone)
		o.streamLogs(logCtx, logger, jobID, containerID, dest)
	}()

	// Wait for container to exit
	exitCode, exitErr := o.waitForExit(ctx, containerID)
	logger = logger.With("exitCode", exitCode)
	if exitErr != nil {
		logger = logger.With("exitError", exitErr)
	}

	// Give logs a moment to flush
	time.Sleep(500 * time.Millisecond)
	logCancel()
	<-logDone

	// Send exit event
	if dest != nil {
		builder := job.NewEventBuilder(jobID, "orchestrator/service", dest.meta)
		event := builder.BuildExitEvent(exitCode, exitErr)
		if job.FilteredEvents(event.Type, dest.events) {
			if err := o.dispatcher.Dispatch(&dispatcher.Event{
				Payload:     event,
				Destination: dest.url,
				SigningKey:  dest.key,
			}); err != nil {
				logger.Warn("Failed to dispatch exit event", "error", err)
			}
		}
	}

	logger.Info("Job completed")

	// Record job completion metrics (skip for resumed jobs where startTime is zero)
	if o.metrics != nil && !startTime.IsZero() {
		duration := time.Since(startTime).Seconds()
		success := exitCode == 0 && exitErr == nil
		o.metrics.RecordJobCompleted(context.Background(), image, success, duration)
	}
}

func (o *Orchestrator) streamLogs(ctx context.Context, logger *slog.Logger, jobID, containerID string, dest *callbackDest) {
	// Skip log streaming if no callback destination or log events not requested
	if dest == nil || !job.FilteredEvents(job.EventTypeLog, dest.events) {
		o.waitForContainerLogs(ctx, containerID)
		return
	}

	logs, err := o.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	})
	if err != nil {
		logger.Error("Failed to get container logs", "error", err)
		return
	}
	defer logs.Close()

	builder := job.NewEventBuilder(jobID, "orchestrator/service", dest.meta)
	header := make([]byte, 8)

	for ctx.Err() == nil {
		if _, err := io.ReadFull(logs, header); err != nil {
			if err != io.EOF && ctx.Err() == nil {
				logger.Debug("Log stream ended", "error", err)
			}
			return
		}

		size := int(header[4])<<24 | int(header[5])<<16 | int(header[6])<<8 | int(header[7])
		if size == 0 {
			continue
		}

		payload := make([]byte, size)
		if _, err := io.ReadFull(logs, payload); err != nil {
			logger.Debug("Failed to read log payload", "error", err)
			return
		}

		stream := "stdout"
		if header[0] == 2 {
			stream = "stderr"
		}

		if lines := splitLines(string(payload)); len(lines) > 0 {
			event := builder.BuildLogEvent(lines, stream)
			// Ignore dispatch errors - dispatcher logs drops internally
			_ = o.dispatcher.Dispatch(&dispatcher.Event{
				Payload:     event,
				Destination: dest.url,
				SigningKey:  dest.key,
			})
		}
	}
}

// waitForContainerLogs consumes logs without sending them (for jobs without log callbacks).
func (o *Orchestrator) waitForContainerLogs(ctx context.Context, containerID string) {
	logs, err := o.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return
	}
	defer logs.Close()

	// Consume logs to prevent Docker from buffering
	_, _ = io.Copy(io.Discard, logs)
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func (o *Orchestrator) waitForExit(ctx context.Context, containerID string) (int, error) {
	statusCh, errCh := o.client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)

	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case err := <-errCh:
		return -1, err
	case status := <-statusCh:
		if status.Error != nil {
			return int(status.StatusCode), fmt.Errorf("%s", status.Error.Message)
		}
		return int(status.StatusCode), nil
	}
}

func (o *Orchestrator) createJobContainer(ctx context.Context, req *job.Request, js *jobState) (string, error) {
	env := make([]string, 0, len(req.Environment))
	for k, v := range req.Environment {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	var cmd []string
	if req.Command != "" {
		cmd = []string{"/bin/sh", "-c", req.Command}
	}

	containerConfig := &container.Config{
		Image:      req.Image,
		Cmd:        cmd,
		Env:        env,
		WorkingDir: "/workspace",
		Labels: map[string]string{
			"job.id":     req.ID,
			"job.type":   "worker",
			"managed-by": "jobs-service",
		},
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: js.volumeName,
				Target: "/workspace",
			},
		},
		Resources: container.Resources{
			NanoCPUs: int64(req.CPU) * 1e9,
			Memory:   int64(req.Memory) * 1024 * 1024,
		},
	}

	containerName := fmt.Sprintf("job-%s-worker", req.ID)
	resp, err := o.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func (o *Orchestrator) createSidecarContainer(ctx context.Context, req *job.Request, js *jobState) (string, error) {
	env := []string{
		fmt.Sprintf("JOB_ID=%s", req.ID),
		fmt.Sprintf("TIMEOUT_SECONDS=%d", req.TimeoutSeconds),
		"SHARED_VOLUME_PATH=/workspace",
	}

	if len(req.Inputs) > 0 {
		inputsJSON, err := json.Marshal(req.Inputs)
		if err != nil {
			return "", fmt.Errorf("failed to marshal inputs: %w", err)
		}
		env = append(env, fmt.Sprintf("INPUTS_JSON=%s", string(inputsJSON)))
	}

	if len(req.Outputs) > 0 {
		outputsJSON, err := json.Marshal(req.Outputs)
		if err != nil {
			return "", fmt.Errorf("failed to marshal outputs: %w", err)
		}
		env = append(env, fmt.Sprintf("OUTPUTS_JSON=%s", string(outputsJSON)))
	}

	if req.Callback != nil && req.Callback.URL != "" {
		callbackURL := req.Callback.URL
		// If proxy URL is configured, route callbacks through orchestrator
		if o.callbackProxyURL != "" {
			callbackURL = fmt.Sprintf("%s/internal/events?url=%s",
				o.callbackProxyURL,
				url.QueryEscape(req.Callback.URL),
			)
		}
		env = append(env, fmt.Sprintf("CALLBACK_URL=%s", callbackURL))
		if req.Callback.Key != "" {
			// Sidecar signs events with this key
			env = append(env, fmt.Sprintf("CALLBACK_KEY=%s", req.Callback.Key))
		}
		if len(req.Callback.Events) > 0 {
			env = append(env, fmt.Sprintf("CALLBACK_EVENTS=%s", strings.Join(req.Callback.Events, ",")))
		}
	}

	if len(req.Meta) > 0 {
		metaJSON, err := json.Marshal(req.Meta)
		if err != nil {
			return "", fmt.Errorf("failed to marshal meta: %w", err)
		}
		env = append(env, fmt.Sprintf("JOB_META=%s", string(metaJSON)))
	}

	containerConfig := &container.Config{
		Image: o.sidecarImage,
		Env:   env,
		Labels: map[string]string{
			"job.id":     req.ID,
			"job.type":   "sidecar",
			"managed-by": "jobs-service",
		},
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: js.volumeName,
				Target: "/workspace",
			},
		},
	}

	containerName := fmt.Sprintf("job-%s-sidecar", req.ID)
	resp, err := o.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func (o *Orchestrator) pullImageIfNeeded(ctx context.Context, imageName string) error {
	_, err := o.client.ImageInspect(ctx, imageName)
	if err == nil {
		return nil
	}

	reader, err := o.client.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(io.Discard, reader)
	return err
}

func (o *Orchestrator) cleanup(ctx context.Context, js *jobState) {
	const stopTimeout = 10

	o.removeContainer(ctx, js.sidecarContainerID, stopTimeout)
	o.removeContainer(ctx, js.jobContainerID, stopTimeout)

	if js.volumeName != "" {
		_ = o.client.VolumeRemove(ctx, js.volumeName, true)
	}
}

func (o *Orchestrator) removeContainer(ctx context.Context, containerID string, stopTimeout int) {
	if containerID == "" {
		return
	}
	_ = o.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &stopTimeout})
	_ = o.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
}

// runMaintenance periodically cleans up expired completed jobs.
func (o *Orchestrator) runMaintenance(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.cleanupExpiredJobs(ctx)
		}
	}
}

// cleanupExpiredJobs removes jobs that completed more than retentionPeriod ago.
func (o *Orchestrator) cleanupExpiredJobs(ctx context.Context) {
	now := time.Now()
	logger := slog.With("component", "maintenance")

	// Collect job IDs and container IDs to check
	type jobInfo struct {
		id          string
		containerID string
	}
	var toCheck []jobInfo

	for jobID, js := range o.state.list() {
		if js == nil {
			continue
		}
		toCheck = append(toCheck, jobInfo{id: jobID, containerID: js.jobContainerID})
	}

	// Check each container's state via Docker
	var expired []string
	for _, j := range toCheck {
		inspect, err := o.client.ContainerInspect(ctx, j.containerID)
		if err != nil {
			expired = append(expired, j.id)
			continue
		}

		if inspect.State.Running {
			continue
		}

		finishedAt, err := time.Parse(time.RFC3339Nano, inspect.State.FinishedAt)
		if err != nil {
			continue
		}
		if now.Sub(finishedAt) > o.retentionPeriod {
			expired = append(expired, j.id)
		}
	}

	if len(expired) == 0 {
		return
	}

	for _, jobID := range expired {
		if js, exists := o.state.release(jobID); exists && js != nil {
			o.cleanup(ctx, js)
			logger.Debug("Cleaned up expired job", "jobId", jobID)
		}
	}

	logger.Info("Maintenance complete", "cleaned", len(expired))
}

// Verify Orchestrator implements job.Orchestrator
var _ job.Orchestrator = (*Orchestrator)(nil)
