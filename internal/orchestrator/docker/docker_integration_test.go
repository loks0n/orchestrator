//go:build integration

package docker

import (
	"context"
	"fmt"
	"orchestrator/internal/dispatcher"
	"orchestrator/internal/job"
	"orchestrator/internal/testutil"
	"testing"
	"time"
)

func TestOrchestrator_Integration(t *testing.T) {
	ctx := context.Background()

	d := dispatcher.NewMemory(dispatcher.MemoryConfig{BufferSize: 100, Workers: 2}, nil)
	defer d.Close(ctx)

	orchestrator, err := NewOrchestrator(ctx, Config{
		SidecarImage: "alpine:latest",
		Dispatcher:   d,
	})
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}
	defer orchestrator.Close()

	jobID := fmt.Sprintf("integration-test-%d", time.Now().UnixNano())

	req := &job.Request{
		ID:             jobID,
		Image:          "alpine:latest",
		Command:        "echo 'hello from integration test'",
		CPU:            1,
		Memory:         128,
		TimeoutSeconds: 60,
	}

	// Run job
	err = orchestrator.Run(ctx, req)
	if err != nil {
		t.Fatalf("Failed to run job: %v", err)
	}

	// Check status
	status, err := orchestrator.Status(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if status.ID != jobID {
		t.Errorf("Expected job ID %s, got %s", jobID, status.ID)
	}

	// Wait for completion
	var finalStatus *job.Status
	testutil.WaitFor(t, func() bool {
		finalStatus, err = orchestrator.Status(ctx, jobID)
		if err != nil {
			return true // Job may have been cleaned up
		}
		return finalStatus.State == job.StateCompleted || finalStatus.State == job.StateFailed
	}, testutil.WithTimeout(30*time.Second), testutil.WithInterval(time.Second))

	// Cleanup
	_ = orchestrator.Stop(ctx, jobID)
}

func TestOrchestrator_List(t *testing.T) {
	ctx := context.Background()

	d := dispatcher.NewMemory(dispatcher.MemoryConfig{BufferSize: 100, Workers: 2}, nil)
	defer d.Close(ctx)

	orchestrator, err := NewOrchestrator(ctx, Config{
		SidecarImage: "alpine:latest",
		Dispatcher:   d,
	})
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}
	defer orchestrator.Close()

	// Create a job
	jobID := fmt.Sprintf("list-test-%d", time.Now().UnixNano())
	req := &job.Request{
		ID:             jobID,
		Image:          "alpine:latest",
		Command:        "sleep 10",
		CPU:            1,
		Memory:         128,
		TimeoutSeconds: 60,
	}

	err = orchestrator.Run(ctx, req)
	if err != nil {
		t.Fatalf("Failed to run job: %v", err)
	}

	// List jobs
	jobs, err := orchestrator.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list jobs: %v", err)
	}

	found := false
	for _, j := range jobs {
		if j.ID == jobID {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Job %s not found in list", jobID)
	}

	// Cleanup
	_ = orchestrator.Stop(ctx, jobID)
}

func TestOrchestrator_Stop(t *testing.T) {
	ctx := context.Background()

	d := dispatcher.NewMemory(dispatcher.MemoryConfig{BufferSize: 100, Workers: 2}, nil)
	defer d.Close(ctx)

	orchestrator, err := NewOrchestrator(ctx, Config{
		SidecarImage: "alpine:latest",
		Dispatcher:   d,
	})
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}
	defer orchestrator.Close()

	jobID := fmt.Sprintf("stop-test-%d", time.Now().UnixNano())
	req := &job.Request{
		ID:             jobID,
		Image:          "alpine:latest",
		Command:        "sleep 300",
		CPU:            1,
		Memory:         128,
		TimeoutSeconds: 60,
	}

	err = orchestrator.Run(ctx, req)
	if err != nil {
		t.Fatalf("Failed to run job: %v", err)
	}

	// Wait for job to start running
	testutil.MustWaitFor(t, func() bool {
		status, err := orchestrator.Status(ctx, jobID)
		if err != nil {
			return false
		}
		return status.State == job.StateRunning
	}, testutil.WithTimeout(30*time.Second), testutil.WithInterval(time.Second))

	// Stop job
	err = orchestrator.Stop(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to stop job: %v", err)
	}

	// Verify job is gone
	_, err = orchestrator.Status(ctx, jobID)
	if err == nil {
		t.Error("Expected error getting status of stopped job")
	}
}
