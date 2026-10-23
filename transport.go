package pacelock

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// validateTransport checks normalized options, not the values originally passed
// to go-redis constructors. It must not connect, instantiate Cluster nodes, or
// change caller-owned options. Wrappers, hooks and custom node factories remain
// the caller's responsibility: redis.Scripter cannot expose their replay policy.
func validateTransport(client redis.Scripter) error {
	switch c := client.(type) {
	case *redis.Client:
		// NewClient converts MaxRetries: -1 to 0; its default becomes 3.
		if c.Options() == nil || c.Options().MaxRetries != 0 {
			return fmt.Errorf("%w: redis.Client requires MaxRetries: -1", ErrInvalid)
		}
	case *redis.ClusterClient:
		// Cluster's outer loop retries network failures as well as MOVED/ASK.
		// MaxRedirects: -1 becomes 0. Node MaxRetries defaults to -1 here and
		// is normalized again by NewClient when each node is instantiated.
		options := c.Options()
		if options == nil || options.MaxRedirects != 0 || options.MaxRetries != -1 {
			return fmt.Errorf("%w: redis.ClusterClient requires MaxRedirects: -1 and MaxRetries: -1", ErrInvalid)
		}
	}
	return nil
}
