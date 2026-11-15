package libv2ray

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	corenet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	core "github.com/xtls/xray-core/core"
	corestats "github.com/xtls/xray-core/features/stats"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
)

// QueryAllOutboundTrafficStats retrieves and resets all outbound traffic counters.
// Returns a single-line text in format: tag,direction,value;tag,direction,value;
// Returns an empty string if the stats manager is not initialized or no counters exist.
func (x *CoreController) QueryAllOutboundTrafficStats() string {
	if x.statsManager == nil {
		return ""
	}

	var b strings.Builder

	x.statsManager.VisitCounters(func(name string, counter corestats.Counter) bool {
		parts := strings.Split(name, ">>>")
		if len(parts) != 4 || parts[0] != "outbound" || parts[2] != "traffic" {
			return true
		}

		tag := parts[1]
		direct := parts[3]
		value := counter.Set(0)
		if value <= 0 {
			return true
		}

		b.WriteString(tag)
		b.WriteByte(',')
		b.WriteString(direct)
		b.WriteByte(',')
		b.WriteString(strconv.FormatInt(value, 10))
		b.WriteByte(';')
		return true
	})
	return b.String()
}

// MeasureDelay measures network latency to a specified URL through the current core instance
// Uses a 12-second timeout context and returns the round-trip time in milliseconds
// An error is returned if the connection fails or returns an unexpected status
func (x *CoreController) MeasureDelay(url string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	return measureInstDelay(ctx, x.coreInstance, url)
}

// MeasureOutboundDelay measures the outbound delay for a given configuration and URL
func MeasureOutboundDelay(ConfigureFileContent string, url string) (int64, error) {
	config, err := coreserial.LoadJSONConfig(strings.NewReader(ConfigureFileContent))
	if err != nil {
		return -1, fmt.Errorf("config load error: %w", err)
	}

	config.Inbound = nil
	var essentialApp []*serial.TypedMessage
	for _, app := range config.App {
		if app.Type == "xray.app.proxyman.OutboundConfig" ||
			app.Type == "xray.app.dispatcher.Config" ||
			app.Type == "xray.app.log.Config" {
			essentialApp = append(essentialApp, app)
		}
	}
	config.App = essentialApp

	inst, err := core.New(config)
	if err != nil {
		return -1, fmt.Errorf("instance creation failed: %w", err)
	}

	if err := inst.Start(); err != nil {
		return -1, fmt.Errorf("startup failed: %w", err)
	}
	defer inst.Close()
	return measureInstDelay(context.Background(), inst, url)
}

// measureInstDelay measures the delay for an instance to a given URL
func measureInstDelay(ctx context.Context, inst *core.Instance, url string) (int64, error) {
	if inst == nil {
		return -1, errors.New("core instance is nil")
	}

	if url == "" {
		url = "https://www.google.com/generate_204"
	}

	tr := &http.Transport{
		TLSHandshakeTimeout: 6 * time.Second,
		DisableKeepAlives:   false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dest, err := corenet.ParseDestination(fmt.Sprintf("%s:%s", network, addr))
			if err != nil {
				return nil, err
			}
			return core.Dial(ctx, inst, dest)
		},
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   12 * time.Second,
	}

	var minDuration int64 = -1
	success := false
	var lastErr error

	defer tr.CloseIdleConnections()

	const attempts = 2
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			if !success {
				return -1, ctx.Err()
			}
			return minDuration, nil
		default:
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create HTTP request: %w", err)
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			lastErr = fmt.Errorf("invalid status: %s", resp.Status)
			continue
		}

		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		duration := time.Since(start).Milliseconds()
		if !success || duration < minDuration {
			minDuration = duration
		}

		success = true
	}
	if !success {
		return -1, lastErr
	}
	return minDuration, nil
}

// SpeedResult carries the outcome of a throughput measurement back to the
// JVM through gomobile. Bytes is the number of payload bytes actually read
// from the response body; ElapsedMs is the wall-clock time of that download.
//
// Failure encoding (mirrors the Kotlin-side sentinels):
//   - Bytes == 0, ElapsedMs == -1: the node could not be reached at all
//     (config load, instance start, dial, TLS, or HTTP status failure).
//   - Bytes > 0, ElapsedMs == -1: the node was reachable and some bytes were
//     transferred, but the download did not complete (timeout / reset).
type SpeedResult struct {
	Bytes     int64
	ElapsedMs int64
}

// MeasureOutboundThroughput downloads the file at url through a throwaway core
// instance built from ConfigureFileContent and reports how many bytes were
// transferred and how long the transfer took. The body is counted, never
// written to disk.
//
// Unlike MeasureOutboundDelay (a latency probe with a hard 12s budget), the
// download is given up to 5 minutes so that large files on slow nodes complete
// instead of tripping a timeout and reporting a false failure.
func MeasureOutboundThroughput(ConfigureFileContent string, url string) (SpeedResult, error) {
	config, err := coreserial.LoadJSONConfig(strings.NewReader(ConfigureFileContent))
	if err != nil {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("config load error: %w", err)
	}

	config.Inbound = nil
	var essentialApp []*serial.TypedMessage
	for _, app := range config.App {
		if app.Type == "xray.app.proxyman.OutboundConfig" ||
			app.Type == "xray.app.dispatcher.Config" ||
			app.Type == "xray.app.log.Config" {
			essentialApp = append(essentialApp, app)
		}
	}
	config.App = essentialApp

	inst, err := core.New(config)
	if err != nil {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("instance creation failed: %w", err)
	}
	if err := inst.Start(); err != nil {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("startup failed: %w", err)
	}
	defer inst.Close()

	if url == "" {
		url = "https://www.google.com/generate_204"
	}

	// Long overall budget: large files on slow nodes must be allowed to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tr := &http.Transport{
		TLSHandshakeTimeout: 15 * time.Second,
		DisableKeepAlives:   false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dest, err := corenet.ParseDestination(fmt.Sprintf("%s:%s", network, addr))
			if err != nil {
				return nil, err
			}
			return core.Dial(ctx, inst, dest)
		},
	}
	defer tr.CloseIdleConnections()

	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Minute,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		// Never reached a usable response: treat as unreachable.
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("invalid status: %s", resp.Status)
	}

	// Count payload bytes without buffering the whole file or touching disk.
	var downloaded int64
	buf := make([]byte, 64*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			downloaded += int64(n)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			// Partial transfer: node was reachable, report what we got as failed.
			return SpeedResult{Bytes: downloaded, ElapsedMs: -1}, fmt.Errorf("download interrupted: %w", readErr)
		}
	}

	elapsed := time.Since(start).Milliseconds()
	if downloaded <= 0 {
		return SpeedResult{Bytes: 0, ElapsedMs: -1}, fmt.Errorf("empty response body")
	}
	return SpeedResult{Bytes: downloaded, ElapsedMs: elapsed}, nil
}
