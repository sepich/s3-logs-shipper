package main

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestKCCSource_Labels(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		labels map[string]string
		ok     bool
	}{
		{"log file", "eks-prod/ns-1/my.connector/AWSLogs/637423518893/KafkaConnectLogs/eu-west-1/976da346-5db5-437d-a65f-da4401162d11-2/2026/10/21/20261021T2135Z_934e9a4b.log.gz",
			map[string]string{"stream": "kcc", "cluster": "eks-prod", "namespace": "ns-1", "connector": "my.connector", "index": "eks-prod-ns-1"}, true},
		{"dir marker", "eks-prod/ns-1/my.connector/AWSLogs/637423518893/", nil, false},
		{"extra prefix", "x/eks-prod/ns-1/my.connector/AWSLogs/637423518893/KafkaConnectLogs/eu-west-1/976da346-2/2026/10/21/20261021T2135Z_934e9a4b.log.gz", nil, false},
		{"alb log", "AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz", nil, false},
	}
	k := &KCCSource{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels, ok, err := k.Labels(tt.key)
			if err != nil || ok != tt.ok || !reflect.DeepEqual(labels, tt.labels) {
				t.Errorf("Labels() = %v, %v, %v; want %v, %v", labels, ok, err, tt.labels, tt.ok)
			}
		})
	}
}

func TestKCCSource_Parse(t *testing.T) {
	type entry struct {
		ts   time.Time
		line string
	}
	key := "c/ns/cr/AWSLogs/637423518893/KafkaConnectLogs/eu-west-1/uuid/2026/10/21/20261021T2135Z_934e9a4b.log.gz"
	fileTs := time.Date(2026, 10, 21, 21, 35, 0, 0, time.UTC)
	ts1 := time.Date(2026, 10, 21, 21, 37, 19, 62000000, time.UTC)
	tests := []struct {
		name   string
		format string
		in     string
		out    []entry
	}{
		{"single", "raw",
			"[Worker-0a14] [2026-10-21 21:37:19,062] INFO started (Foo:1)",
			[]entry{{ts1, "[Worker-0a14] [2026-10-21 21:37:19,062] INFO started (Foo:1)"}}},
		{"leading lines use file ts", "raw",
			"[Worker-0a14] Waiting for DNS\n[Worker-0a14] DNS available\n[Worker-0a14] [2026-10-21 21:37:19,062] INFO started",
			[]entry{{fileTs.Add(1), "[Worker-0a14] Waiting for DNS"}, {fileTs.Add(2), "[Worker-0a14] DNS available"}, {ts1, "[Worker-0a14] [2026-10-21 21:37:19,062] INFO started"}}},
		{"interleaved workers", "raw",
			"[Worker-0a14] [2026-10-21 21:37:19,062] INFO Config values:\n[Worker-0b30] Waiting for DNS\n[Worker-0a14] \ta = 1",
			[]entry{{ts1, "[Worker-0a14] [2026-10-21 21:37:19,062] INFO Config values:"}, {ts1.Add(1), "[Worker-0b30] Waiting for DNS"}, {ts1.Add(2), "[Worker-0a14] \ta = 1"}}},
		{"same ms after untimed lines", "raw",
			"[Worker-0a14] [2026-10-21 21:37:19,062] INFO Config values:\n[Worker-0a14] \ta = 1\n[Worker-0a14] [2026-10-21 21:37:19,062] INFO next\n[Worker-0a14] [2026-10-21 21:37:19,061] INFO earlier ms",
			[]entry{{ts1, "[Worker-0a14] [2026-10-21 21:37:19,062] INFO Config values:"}, {ts1.Add(1), "[Worker-0a14] \ta = 1"}, {ts1.Add(2), "[Worker-0a14] [2026-10-21 21:37:19,062] INFO next"}, {ts1.Add(-time.Millisecond), "[Worker-0a14] [2026-10-21 21:37:19,061] INFO earlier ms"}}},
		{"no worker prefix", "raw",
			"[2026-10-21 21:37:19,062] INFO started",
			[]entry{{ts1, "[2026-10-21 21:37:19,062] INFO started"}}},
		{"json", "json",
			"[Worker-0a14] [2026-10-21 21:37:19,062] INFO \"quoted\"\t1",
			[]entry{{ts1, `{"line":"[Worker-0a14] [2026-10-21 21:37:19,062] INFO \"quoted\"\t1"}`}}},
		{"json no html escape", "json",
			"[Worker-0a14] java.lang.NoSuchMethodException: Foo.<init>() & more",
			[]entry{{fileTs.Add(1), `{"line":"[Worker-0a14] java.lang.NoSuchMethodException: Foo.<init>() & more"}`}}},
		{"empty", "raw", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out []entry
			err := (&KCCSource{format: tt.format}).Parse(key, bufio.NewScanner(strings.NewReader(tt.in)), func(ts time.Time, line string) error {
				out = append(out, entry{ts, line})
				return nil
			})
			if err != nil || !reflect.DeepEqual(out, tt.out) {
				t.Errorf("Parse() = %+v, %v; want %+v", out, err, tt.out)
			}
		})
	}
}
