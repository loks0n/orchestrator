// Package sidecar implements the job sidecar that handles input downloads and output processing.
package sidecar

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// InputHandler handles downloading input files
type InputHandler struct {
	httpClient *http.Client
}

// NewInputHandler creates a new input handler
func NewInputHandler(timeout time.Duration) *InputHandler {
	return &InputHandler{
		httpClient: &http.Client{Timeout: timeout},
	}
}

// Download downloads a file from the given URL to the specified path
func (h *InputHandler) Download(ctx context.Context, url, destPath string) error {
	// Ensure the destination directory exists
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create the request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Execute the request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	// Create the destination file
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", destPath, err)
	}
	defer file.Close()

	// Copy the response body to the file
	written, err := io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Sync to ensure the file is written
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	slog.Debug("Downloaded file", "bytes", written, "path", destPath)
	return nil
}

// WriteFile writes content directly to a file
func (h *InputHandler) WriteFile(destPath, content string) error {
	// Ensure the destination directory exists
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write the content to the file
	if err := os.WriteFile(destPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", destPath, err)
	}

	slog.Debug("Wrote file", "bytes", len(content), "path", destPath)
	return nil
}
