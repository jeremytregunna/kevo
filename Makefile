.PHONY: all build clean

all: build

build:
	go build -o kevo ./cmd/kevo

clean:
	rm -f kevo
