VERSION=$(shell echo "$$(git rev-parse --abbrev-ref HEAD)-$$(git rev-parse --short HEAD)")
GOBUILD=go build -ldflags "-w -s -X git.umlife.net/adxmi/adn.version=$(VERSION)"

build:
	$(GOBUILD) -o bin/sqlpipe/sqlpipe github.com/nicolasq123/sqlpipe/cmd