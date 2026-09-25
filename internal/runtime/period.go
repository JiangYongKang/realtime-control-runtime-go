package runtime

import "time"

func periodFromNanos(n int64) time.Duration { return time.Duration(n) }
