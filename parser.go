package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Source defines a log type detected by S3 key
type Source interface {
	// Labels returns stream labels and true if the key belongs to this source
	Labels(key string) (map[string]string, bool, error)
	// Parse reads log lines and emits entries to ship
	Parse(key string, scanner *bufio.Scanner, emit func(time.Time, string) error) error
}

type Parser struct {
	opts     Options
	sources  []Source
	s3Client *s3.Client
	logger   *slog.Logger
	queue    chan *string
	stop     bool
}

func NewParser(opts Options, elbMeta *ELBMeta, s3Client *s3.Client, logger *slog.Logger) *Parser {
	parser := &Parser{
		opts: opts,
		sources: []Source{
			&ALBSource{meta: elbMeta, line: &LineSlice{}, format: opts.ALBFormat},
			&KCCSource{},
		},
		s3Client: s3Client,
		logger:   logger,
		queue:    make(chan *string, 10*opts.Workers),
	}
	return parser
}

// Stop gracefully all workers
func (s *Parser) Stop() {
	if s.stop {
		return
	}
	s.stop = true
	close(s.queue)
}

func (s *Parser) scan() error {
	num := 0
	ctx := context.Background()
	maxKeys := int32(1000) //no pager, tune interval to have less files per run
	output, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s.opts.BucketName,
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return err
	}

	start := time.Now()
	for _, obj := range output.Contents {
		if obj.Key == nil || s.stop {
			continue
		}
		s.queue <- obj.Key
		num++
	}
	if num > 0 {
		s.logger.Info("new files", "found", num, "duration", time.Since(start), "queue", len(s.queue))
	}
	return nil
}

func (s *Parser) worker() error {
	ctx := context.Background() // limit time to process file? will restart of processing help?

	for fn := range s.queue {
		skip, err := s.process(ctx, *fn)
		if err != nil {
			s.logger.Error("failed to ship file", "key", *fn, "err", err)
			return err // pod restart instead of deletion of not-shipped file
		}
		if skip {
			continue
		}

		if _, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &s.opts.BucketName,
			Key:    fn,
		}); err != nil {
			s.logger.Error("failed to delete file", "key", *fn, "err", err)
		}
	}
	return nil
}

// process ships the file, returns true if it is unknown and should be kept in the bucket
func (s *Parser) process(ctx context.Context, fn string) (bool, error) {
	if strings.HasSuffix(fn, "/") {
		return false, nil // dir markers are just deleted
	}
	for _, src := range s.sources {
		if labels, ok, err := src.Labels(fn); ok {
			if err != nil {
				return false, err
			}
			return false, s.parseFile(ctx, fn, src, labels)
		}
	}
	s.logger.Debug("skipping unknown log file", "key", fn)
	return true, nil
}

func (s *Parser) parseFile(ctx context.Context, fn string, src Source, labels map[string]string) error {
	start := time.Now()
	for k, v := range s.opts.Labels {
		if v == "" {
			delete(labels, k)
		} else {
			labels[k] = v
		}
	}
	b := newBatch(labels, s.opts, s.logger)

	obj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.opts.BucketName,
		Key:    &fn,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			s.logger.Debug("skipping non-existent file", "key", fn)
			return nil
		}
		return fmt.Errorf("failed to get object %s: %w", fn, err)
	}
	defer obj.Body.Close()

	gzreader, err := gzip.NewReader(obj.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzreader.Close()

	var lineCount int
	scanner := bufio.NewScanner(gzreader)
	if err = src.Parse(fn, scanner, func(ts time.Time, line string) error {
		lineCount++
		if err := b.add(ts, line); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan file %s: %w", fn, err)
	}
	if err = b.flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	s.logger.Debug("shipped file", "key", fn, "labels", fmt.Sprintf("%v", labels), "lines", lineCount, "duration", time.Since(start), "lines/s", fmt.Sprintf("%.2f", float64(lineCount)/time.Since(start).Seconds()))
	return nil
}

func (s *Parser) metrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "s3_logs_shipper_queue_length %d\n", len(s.queue))
	})
}
