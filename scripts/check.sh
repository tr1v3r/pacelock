#!/bin/sh
# Run from any directory; backend integration is selected by PACELOCK_* variables.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
    printf '%s\n' "$unformatted"
    exit 1
fi
for module in . drivers/redis drivers/postgres tests/contracts examples/postgres; do
    (
        cd "$root/$module"
        go mod verify
        go vet ./...
        go test -race -count=1 -timeout=5m "$@" ./...
    )
done
