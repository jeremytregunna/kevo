.PHONY: all build clean

all: build

build:
	go build -o gs ./cmd/gs

clean:
	rm -f gs
