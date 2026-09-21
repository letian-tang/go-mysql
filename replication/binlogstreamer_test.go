package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinlogStreamerDrainsQueuedEventBeforeError(t *testing.T) {
	streamer := NewBinlogStreamerWithChanSize(1)
	event := &BinlogEvent{Header: &EventHeader{}, Event: &FailoverBoundaryEvent{}}
	wantErr := errors.New("stream error")

	require.NoError(t, streamer.AddEventToStreamer(event))
	require.True(t, streamer.AddErrorToStreamer(wantErr))

	got, err := streamer.GetEvent(context.Background())
	require.NoError(t, err)
	require.Same(t, event, got)

	got, err = streamer.GetEvent(context.Background())
	require.Nil(t, got)
	require.ErrorIs(t, err, wantErr)
}

func TestBinlogStreamerDrainsQueuedEventBeforePendingError(t *testing.T) {
	streamer := NewBinlogStreamerWithChanSize(1)
	event := &BinlogEvent{Header: &EventHeader{}, Event: &FailoverBoundaryEvent{}}
	wantErr := errors.New("stream error")

	streamer.pendingErr = wantErr
	streamer.ch <- event

	got, err := streamer.GetEvent(context.Background())
	require.NoError(t, err)
	require.Same(t, event, got)

	got, err = streamer.GetEvent(context.Background())
	require.Nil(t, got)
	require.ErrorIs(t, err, wantErr)
}

func TestAddEventToStreamerContextStopsWhenCanceled(t *testing.T) {
	streamer := NewBinlogStreamerWithChanSize(1)
	require.NoError(t, streamer.AddEventToStreamer(&BinlogEvent{}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := streamer.AddEventToStreamerContext(ctx, &BinlogEvent{})
	require.ErrorIs(t, err, context.Canceled)
}
