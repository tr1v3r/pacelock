module github.com/tr1v3r/pacelock/tests/contracts

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.9.2
	github.com/redis/go-redis/v9 v9.23.0
	github.com/tr1v3r/pacelock v0.0.0-20261027161106-9ff3e7a851af
	github.com/tr1v3r/pacelock/driver/postgres v0.0.0
	github.com/tr1v3r/pacelock/driver/redis v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/tr1v3r/pacelock => ../..

replace github.com/tr1v3r/pacelock/driver/redis => ../../driver/redis

replace github.com/tr1v3r/pacelock/driver/postgres => ../../driver/postgres
