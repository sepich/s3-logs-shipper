package main

import (
	"bufio"
	"encoding/json"
	"path"
	"regexp"
	"strings"
	"time"
)

// format:  <cluster>/<namespace>/<cr-name>/AWSLogs/<account-id>/KafkaConnectLogs/<region>/<connector-uuid>/<YYYY>/<MM>/<DD>/<YYYYMMDD>T<HHMM>Z_<rand>.log.gz
// example: minikube-local/default/kcc-e2e-01-json/AWSLogs/637423518893/KafkaConnectLogs/eu-west-1/976da346-5db5-437d-a65f-da4401162d11-2/2026/10/21/20261021T2135Z_934e9a4b.log.gz
var kccRegex = regexp.MustCompile(`^(?P<cluster>[^/]+)/(?P<namespace>[^/]+)/(?P<connector>[^/]+)/AWSLogs/\d{12}/KafkaConnectLogs/[^/]+/[^/]+/\d{4}/\d{2}/\d{2}/\d{8}T\d{4}Z_[^/]+\.log\.gz$`)

// KCCSource ships MSK Connect worker logs, labels are taken from the key prefix set by KafkaConnectController
type KCCSource struct {
	format string
}

var _ Source = &KCCSource{}

func (k *KCCSource) Labels(key string) (map[string]string, bool, error) {
	matches := kccRegex.FindStringSubmatch(key)
	if len(matches) == 0 {
		return nil, false, nil
	}
	cluster, namespace := matches[1], matches[2]
	return map[string]string{
		"cluster":   cluster,
		"namespace": namespace,
		"connector": matches[3],
		"index":     cluster + "-" + namespace,
	}, true, nil
}

// Parse ships lines as is, lines without timestamp get previous one +1ns
func (k *KCCSource) Parse(key string, scanner *bufio.Scanner, emit func(time.Time, string) error) error {
	ts, err := time.Parse("20060102T1504Z", path.Base(key)[:14]) // for leading lines without timestamp
	if err != nil {
		return err
	}
	for scanner.Scan() {
		line := scanner.Text()
		if t, ok := kccTime(line); ok && (t.After(ts) || ts.Sub(t) >= time.Millisecond) { // keep order within the same ms
			ts = t
		} else {
			ts = ts.Add(time.Nanosecond)
		}
		if err := emit(ts, k.as(line)); err != nil {
			return err
		}
	}
	return nil
}

func (k *KCCSource) as(line string) string {
	if k.format != "json" {
		return line
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false) // keep `<init>` searchable by line filters
	_ = enc.Encode(map[string]string{"line": line})
	return strings.TrimSuffix(b.String(), "\n")
}

// kccTime parses log4j timestamp in UTC: [Worker-0a14c4caf9f5b2e2e] [2026-10-21 21:37:19,062]
func kccTime(line string) (time.Time, bool) {
	if strings.HasPrefix(line, "[Worker-") {
		_, line, _ = strings.Cut(line, "] ")
	}
	if len(line) < 25 || line[0] != '[' || line[24] != ']' {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02 15:04:05,000", line[1:24])
	return t, err == nil
}
