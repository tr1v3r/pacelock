module github.com/tr1v3r/pacelock/drivers/redis

go 1.26.0

require github.com/redis/go-redis/v9 v9.22.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

require github.com/tr1v3r/pacelock v0.0.0-20261027041359-ff7d850a7bd5

replace github.com/tr1v3r/pacelock => ../..
