run:
	go run bloom.go boltdb.go db.go scalable_bloom.go main.go

test:
	go test ./...

build:
	go build -o .