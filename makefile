# Makefile

export GOPATH := $(shell pwd)

all:
	echo $$GOPATH
	go get -d
	go run *.go

build:
	echo $$GOPATH
	go get -d
go build -o server.bin
