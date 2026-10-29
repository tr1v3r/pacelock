module github.com/tr1v3r/pacelock/examples/redis

go 1.26.0

require (
	github.com/redis/go-redis/v9 v9.23.0
	github.com/tr1v3r/pacelock v0.0.0
	github.com/tr1v3r/pacelock/driver/redis v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/tr1v3r/pacelock => ../..

replace github.com/tr1v3r/pacelock/driver/redis => ../../driver/redis
