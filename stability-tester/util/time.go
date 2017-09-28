package util

import "time"

// Duration is for parsing duration to time.Duration in toml.
type Duration struct {
	time.Duration
}

// UnmarshalText implements toml UnmarshalText interface.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
