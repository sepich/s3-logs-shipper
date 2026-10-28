package main

import (
	"bufio"
	"fmt"
	"regexp"
	"time"
)

var (
	// source:  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
	// format:  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
	// example: my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz
	fnRegex    = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/elasticloadbalancing\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_elasticloadbalancing_(?:\w+-\w+-(?:\w+-)?\d)_app\.(?P<id>[a-zA-Z0-9\-]+)\..+\.log\.gz`)
	evRegex    = regexp.MustCompile(`(?P<type>\S+) (?P<time>\S+) (?P<elb>\S+) (?P<client>\S+) (?P<target>\S+) (?P<request_processing_time>\S+) (?P<target_processing_time>\S+) (?P<response_processing_time>\S+) (?P<elb_status_code>\S+) (?P<target_status_code>\S+) (?P<received_bytes>\S+) (?P<sent_bytes>\S+) (?P<request>".+") (?P<user_agent>".*") (?P<ssl_cipher>\S+) (?P<ssl_protocol>\S+) (?P<target_group_arn>\S+) (?P<trace_id>".+") (?P<domain_name>".+") (?P<chosen_cert_arn>".+") (?P<matched_rule_priority>\S+) (?P<request_creation_time>\S+) (?P<actions_executed>".+") (?P<redirect_url>".+") (?P<error_reason>".+") (?P<targets>".+") (?P<target_status_code_list>".+") (?P<classification>".+") (?P<classification_reason>".+") (?P<conn_trace_id>\S+)`)
	skipFields = map[string]bool{
		"chosen_cert_arn":         true, // hardcoded in ingress
		"target_group_arn":        true, // not configured directly
		"matched_rule_priority":   true, // not configured directly
		"error_reason":            true, // only for lambda
		"targets":                 true, // same as target
		"target_status_code_list": true, // same as target_status_code
		"classification":          true, // not used
		"classification_reason":   true, // not used
		"conn_trace_id":           true, // only for connection logs
	}
	quoteFields = map[string]bool{
		"request":                 true,
		"user_agent":              true,
		"trace_id":                true,
		"domain_name":             true,
		"chosen_cert_arn":         true,
		"actions_executed":        true,
		"redirect_url":            true,
		"error_reason":            true,
		"targets":                 true,
		"target_status_code_list": true,
		"classification":          true,
		"classification_reason":   true,
	}
	numFields = map[string]bool{
		"elb_status_code":          true,
		"received_bytes":           true,
		"request_processing_time":  true,
		"response_processing_time": true,
		"sent_bytes":               true,
		"target_processing_time":   true,
		"target_status_code":       true,
	}
)

// ALBSource ships ALB access logs, labels are taken from ALB tags
type ALBSource struct {
	meta   *ELBMeta
	line   LineParser
	format string
}

var _ Source = &ALBSource{}

func (a *ALBSource) Labels(key string) (map[string]string, bool, error) {
	matches := fnRegex.FindStringSubmatch(key)
	if len(matches) == 0 {
		return nil, false, nil
	}
	accountID, lb := matches[fnRegex.SubexpIndex("account_id")], matches[fnRegex.SubexpIndex("id")]
	meta, err := a.meta.Get(accountID, lb)
	if err != nil {
		return nil, true, fmt.Errorf("failed to get metadata for load balancer %s/%s: %w", accountID, lb, err)
	}
	labels := map[string]string{
		"namespace": meta.Namespace,
		"ingress":   meta.Ingress,
	}
	if meta.Cluster != "" {
		labels["cluster"] = meta.Cluster
		labels["index"] = meta.Cluster + "-" + meta.Namespace
	}
	return labels, true, nil
}

func (a *ALBSource) Parse(_ string, scanner *bufio.Scanner, emit func(time.Time, string) error) error {
	for scanner.Scan() {
		ts, line, err := a.line.As(a.format, scanner.Text())
		if err != nil {
			return err
		}
		if err = emit(*ts, line); err != nil {
			return err
		}
	}
	return nil
}
