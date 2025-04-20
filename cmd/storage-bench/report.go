package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// BenchmarkResult stores the results of a benchmark
type BenchmarkResult struct {
	BenchmarkType string
	NumKeys       int
	ValueSize     int
	Mode          string
	Operations    int
	Duration      float64
	Throughput    float64
	Latency       float64
	HitRate       float64 // For read benchmarks
	EntriesPerSec float64 // For scan benchmarks
	ReadRatio     float64 // For mixed benchmarks
	WriteRatio    float64 // For mixed benchmarks
	Timestamp     time.Time
}

// SaveResultCSV saves benchmark results to a CSV file
func SaveResultCSV(results []BenchmarkResult, filename string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"Timestamp", "BenchmarkType", "NumKeys", "ValueSize", "Mode",
		"Operations", "Duration", "Throughput", "Latency", "HitRate",
		"EntriesPerSec", "ReadRatio", "WriteRatio",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write results
	for _, r := range results {
		record := []string{
			r.Timestamp.Format(time.RFC3339),
			r.BenchmarkType,
			strconv.Itoa(r.NumKeys),
			strconv.Itoa(r.ValueSize),
			r.Mode,
			strconv.Itoa(r.Operations),
			fmt.Sprintf("%.2f", r.Duration),
			fmt.Sprintf("%.2f", r.Throughput),
			fmt.Sprintf("%.3f", r.Latency),
			fmt.Sprintf("%.2f", r.HitRate),
			fmt.Sprintf("%.2f", r.EntriesPerSec),
			fmt.Sprintf("%.1f", r.ReadRatio),
			fmt.Sprintf("%.1f", r.WriteRatio),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// LoadResultCSV loads benchmark results from a CSV file
func LoadResultCSV(filename string) ([]BenchmarkResult, error) {
	// Open file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Skip header
	if len(records) <= 1 {
		return []BenchmarkResult{}, nil
	}
	records = records[1:]

	// Parse results
	results := make([]BenchmarkResult, 0, len(records))
	for _, record := range records {
		if len(record) < 13 {
			continue
		}

		timestamp, _ := time.Parse(time.RFC3339, record[0])
		numKeys, _ := strconv.Atoi(record[2])
		valueSize, _ := strconv.Atoi(record[3])
		operations, _ := strconv.Atoi(record[5])
		duration, _ := strconv.ParseFloat(record[6], 64)
		throughput, _ := strconv.ParseFloat(record[7], 64)
		latency, _ := strconv.ParseFloat(record[8], 64)
		hitRate, _ := strconv.ParseFloat(record[9], 64)
		entriesPerSec, _ := strconv.ParseFloat(record[10], 64)
		readRatio, _ := strconv.ParseFloat(record[11], 64)
		writeRatio, _ := strconv.ParseFloat(record[12], 64)

		result := BenchmarkResult{
			Timestamp:     timestamp,
			BenchmarkType: record[1],
			NumKeys:       numKeys,
			ValueSize:     valueSize,
			Mode:          record[4],
			Operations:    operations,
			Duration:      duration,
			Throughput:    throughput,
			Latency:       latency,
			HitRate:       hitRate,
			EntriesPerSec: entriesPerSec,
			ReadRatio:     readRatio,
			WriteRatio:    writeRatio,
		}
		results = append(results, result)
	}

	return results, nil
}

// PrintResultTable prints a formatted table of benchmark results
func PrintResultTable(results []BenchmarkResult) {
	if len(results) == 0 {
		fmt.Println("No results to display")
		return
	}

	// Print header
	fmt.Println("+-----------------+--------+---------+------------+----------+----------+")
	fmt.Println("| Benchmark Type  | Keys   | ValSize | Throughput | Latency  | Hit Rate |")
	fmt.Println("+-----------------+--------+---------+------------+----------+----------+")

	// Print results
	for _, r := range results {
		hitRateStr := "-"
		if r.BenchmarkType == "Read" {
			hitRateStr = fmt.Sprintf("%.2f%%", r.HitRate)
		} else if r.BenchmarkType == "Mixed" {
			hitRateStr = fmt.Sprintf("R:%.0f/W:%.0f", r.ReadRatio, r.WriteRatio)
		}

		latencyUnit := "µs"
		latency := r.Latency
		if latency > 1000 {
			latencyUnit = "ms"
			latency /= 1000
		}

		fmt.Printf("| %-15s | %6d | %7d | %10.2f | %6.2f%s | %8s |\n",
			r.BenchmarkType,
			r.NumKeys,
			r.ValueSize,
			r.Throughput,
			latency, latencyUnit,
			hitRateStr)
	}
	fmt.Println("+-----------------+--------+---------+------------+----------+----------+")
}
