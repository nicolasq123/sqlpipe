GOBUILD=go build

build:
	$(GOBUILD) -o bin/sqlpipe/sqlpipe github.com/nicolasq123/sqlpipe/cmd

run:
	bin/sqlpipe/sqlpipe --conf "./cmd/conf.yml"

build_run: \
	build \
	run \
