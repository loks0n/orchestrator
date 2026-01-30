package job

import (
	"fmt"
	"orchestrator/pkg/cloudevent"
	"slices"
	"time"
)

// Event types for job lifecycle callbacks
const (
	EventTypeStart  = "orchestrator.job.start"
	EventTypeInput  = "orchestrator.job.input"
	EventTypeLog    = "orchestrator.job.log"
	EventTypeExit   = "orchestrator.job.exit"
	EventTypeOutput = "orchestrator.job.output"
)

// FilteredEvents returns true if the event type should be sent based on the filter.
// If the filter is empty, all events are allowed.
func FilteredEvents(eventType string, filter []string) bool {
	if len(filter) == 0 {
		return true
	}
	return slices.Contains(filter, eventType)
}

// EventBuilder builds CloudEvents for job lifecycle events.
type EventBuilder struct {
	source  string
	subject string
	meta    map[string]string
}

// NewEventBuilder creates a new EventBuilder.
func NewEventBuilder(jobID, source string, meta map[string]string) *EventBuilder {
	return &EventBuilder{
		source:  source,
		subject: jobID,
		meta:    meta,
	}
}

// Build creates a new CloudEvent with the given type and data.
func (b *EventBuilder) Build(eventType string, data map[string]any) *cloudevent.CloudEvent {
	eventID := fmt.Sprintf("%s-%d", b.subject, time.Now().UnixNano())
	return cloudevent.New(eventType, b.source, b.subject, eventID, data)
}

// BuildStartEvent creates a job start event.
func (b *EventBuilder) BuildStartEvent() *cloudevent.CloudEvent {
	data := map[string]any{
		"jobId": b.subject,
		"meta":  b.meta,
	}
	return b.Build(EventTypeStart, data)
}

// BuildInputEvent creates an input event.
func (b *EventBuilder) BuildInputEvent(inputID, status, path string, err error) *cloudevent.CloudEvent {
	data := map[string]any{
		"jobId":   b.subject,
		"inputId": inputID,
		"status":  status,
		"path":    path,
		"meta":    b.meta,
	}
	if err != nil {
		data["error"] = err.Error()
	}
	return b.Build(EventTypeInput, data)
}

// BuildLogEvent creates a log event.
func (b *EventBuilder) BuildLogEvent(lines []string, stream string) *cloudevent.CloudEvent {
	data := map[string]any{
		"jobId":  b.subject,
		"lines":  lines,
		"stream": stream,
		"meta":   b.meta,
	}
	return b.Build(EventTypeLog, data)
}

// BuildExitEvent creates an exit event.
func (b *EventBuilder) BuildExitEvent(exitCode int, err error) *cloudevent.CloudEvent {
	data := map[string]any{
		"jobId":    b.subject,
		"exitCode": exitCode,
		"meta":     b.meta,
	}
	if err != nil {
		data["error"] = err.Error()
	}
	return b.Build(EventTypeExit, data)
}

// BuildOutputEvent creates an output event.
func (b *EventBuilder) BuildOutputEvent(outputID, outputType, status string, content any, err error) *cloudevent.CloudEvent {
	data := map[string]any{
		"jobId":      b.subject,
		"outputId":   outputID,
		"outputType": outputType,
		"status":     status,
		"meta":       b.meta,
	}
	if content != nil {
		data["content"] = content
	}
	if err != nil {
		data["error"] = err.Error()
	}
	return b.Build(EventTypeOutput, data)
}
