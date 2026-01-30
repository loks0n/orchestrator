package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"orchestrator/internal/job"
	"orchestrator/pkg/cloudevent"
	"os"
	"strings"
	"time"
)

// Runner orchestrates the sidecar flow.
// The sidecar handles:
//   - Input processing (downloads, file writes) with callbacks
//   - Output processing (uploads, events) with callbacks
//
// Log streaming, start, and exit events are handled by the supervisor.
type Runner struct {
	config        *Config
	events        []string // parsed from config
	inputs        []job.Input
	outputs       []job.Output
	sender        *cloudevent.Sender
	eventBuilder  *job.EventBuilder
	inputHandler  *InputHandler
	outputHandler *OutputHandler
}

// NewRunner creates a new sidecar runner.
func NewRunner(cfg *Config) (*Runner, error) {
	var events []string
	if cfg.CallbackEvents != "" {
		events = strings.Split(cfg.CallbackEvents, ",")
	}

	var inputs []job.Input
	if cfg.InputsJSON != "" && cfg.InputsJSON != "[]" {
		if err := json.Unmarshal([]byte(cfg.InputsJSON), &inputs); err != nil {
			return nil, fmt.Errorf("failed to parse inputs: %w", err)
		}
	}

	var outputs []job.Output
	if cfg.OutputsJSON != "" && cfg.OutputsJSON != "[]" {
		if err := json.Unmarshal([]byte(cfg.OutputsJSON), &outputs); err != nil {
			return nil, fmt.Errorf("failed to parse outputs: %w", err)
		}
	}

	var meta map[string]string
	if cfg.Meta != "" {
		_ = json.Unmarshal([]byte(cfg.Meta), &meta)
	}

	return &Runner{
		config:        cfg,
		events:        events,
		inputs:        inputs,
		outputs:       outputs,
		sender:        cloudevent.NewSender(cfg.CallbackTimeout),
		eventBuilder:  job.NewEventBuilder(cfg.JobID, "orchestrator/sidecar", meta),
		inputHandler:  NewInputHandler(time.Duration(cfg.TimeoutSeconds) * time.Second),
		outputHandler: NewOutputHandler(cfg.UploadTimeout, cfg.UploadRetries),
	}, nil
}

// Run executes the sidecar flow:
// 1. Process inputs (downloads, file writes)
// 2. Wait for each output to exist and process it
func (r *Runner) Run(ctx context.Context) error {
	logger := slog.With("jobId", r.config.JobID, "inputs", len(r.inputs), "outputs", len(r.outputs))

	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.config.TimeoutSeconds)*time.Second)
	defer cancel()

	if err := r.handleInputs(ctx); err != nil {
		logger = logger.With("inputError", err)
	}

	r.processOutputs(ctx)

	logger.Info("Sidecar completed")
	return nil
}

// sendEvent sends a CloudEvent if the event type is in the filter.
func (r *Runner) sendEvent(ctx context.Context, event *cloudevent.CloudEvent) error {
	if r.config.CallbackURL == "" {
		return nil
	}
	if !job.FilteredEvents(event.Type, r.events) {
		return nil
	}

	opts := cloudevent.SendOptions{}
	if r.config.CallbackKey != "" {
		signature, err := cloudevent.Sign(event, r.config.CallbackKey)
		if err != nil {
			return fmt.Errorf("failed to sign event: %w", err)
		}
		opts.Signature = signature
	}

	return r.sender.Send(ctx, r.config.CallbackURL, event, opts)
}

func (r *Runner) handleInputs(ctx context.Context) error {
	var lastErr error
	for _, input := range r.inputs {
		err := r.processInput(ctx, input)
		status := "success"
		if err != nil {
			status = "failed"
			lastErr = err
		}

		event := r.eventBuilder.BuildInputEvent(input.ID, status, input.Path, err)
		sendErr := r.sendEvent(ctx, event)

		logger := slog.With("inputId", input.ID, "type", input.Type, "status", status)
		if err != nil {
			logger = logger.With("error", err)
		}
		if sendErr != nil {
			logger = logger.With("callbackError", sendErr)
		}
		logger.Debug("Input processed")
	}
	return lastErr
}

func (r *Runner) processInput(ctx context.Context, input job.Input) error {
	destPath := r.config.SharedVolumePath + "/" + input.Path

	switch input.Type {
	case "download":
		return r.inputHandler.Download(ctx, input.URL, destPath)
	case "file":
		return r.inputHandler.WriteFile(destPath, input.Content)
	default:
		slog.With("inputId", input.ID, "type", input.Type).Warn("Input skipped", "reason", "unsupported type")
		return nil
	}
}

func (r *Runner) processOutputs(ctx context.Context) {
	for _, output := range r.outputs {
		r.processOutput(ctx, output)
	}
}

func (r *Runner) processOutput(ctx context.Context, output job.Output) {
	logger := slog.With("outputId", output.ID, "type", output.Type, "path", output.Path)
	fullPath := r.config.SharedVolumePath + "/" + output.Path

	if err := r.waitForFile(ctx, fullPath); err != nil {
		event := r.eventBuilder.BuildOutputEvent(output.ID, output.Type, "failed", nil, err)
		sendErr := r.sendEvent(ctx, event)
		logger = logger.With("error", err, "status", "failed")
		if sendErr != nil {
			logger = logger.With("callbackError", sendErr)
		}
		logger.Warn("Output failed")
		return
	}

	outputWithPath := output
	outputWithPath.Path = fullPath
	result := r.outputHandler.Process(ctx, outputWithPath)

	event := r.eventBuilder.BuildOutputEvent(output.ID, output.Type, result.Status, result.Content, result.Error)
	sendErr := r.sendEvent(ctx, event)

	logger = logger.With("status", result.Status)
	if result.Error != nil {
		logger = logger.With("error", result.Error)
	}
	if sendErr != nil {
		logger = logger.With("callbackError", sendErr)
	}
	logger.Debug("Output processed")
}

func (r *Runner) waitForFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				return nil
			}
		}
	}
}

// Close releases resources.
func (r *Runner) Close() error {
	return nil
}
