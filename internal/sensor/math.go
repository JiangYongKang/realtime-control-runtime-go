package sensor

import "math"

// sin2pi returns sin(2*pi*x) without importing math everywhere.
func sin2pi(x float64) float64 { return math.Sin(2 * math.Pi * x) }
