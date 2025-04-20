# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `go build ./...`
- Run tests: `go test ./...`
- Run single test: `go test ./pkg/path/to/package -run TestName`
- Benchmark: `go test ./pkg/path/to/package -bench .`
- Race detector: `go test -race ./...`

## Linting/Formatting
- Format code: `go fmt ./...`
- Static analysis: `go vet ./...`
- Install golangci-lint: `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`
- Run linter: `golangci-lint run`

## Code Style Guidelines
- Follow Go standard project layout in pkg/ and internal/ directories
- Use descriptive error types with context wrapping
- Implement single-writer architecture for write paths
- Allow concurrent reads via snapshots
- Use interfaces for component boundaries
- Follow idiomatic Go practices
- Add appropriate validation, especially for checksums
- All exported functions must have documentation comments
- For transaction management, use WAL for durability/atomicity

## Version Control
- Use git for version control
- All commit messages must use semantic commit messages
- All commit messages must not reference code being generated or co-authored by Claude
