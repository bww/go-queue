package config

type Config struct {
	Debug, Verbose bool
	Backlog        int
	Synchronous    bool
}

func (c Config) WithOptions(opts ...Option) Config {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type Option func(Config) Config

func Debug(on bool) Option {
	return func(c Config) Config {
		c.Debug = on
		return c
	}
}

func Verbose(on bool) Option {
	return func(c Config) Config {
		c.Verbose = on
		return c
	}
}

func Backlog(n int) Option {
	return func(c Config) Config {
		c.Backlog = n
		return c
	}
}

func Synchronous(on bool) Option {
	return func(c Config) Config {
		c.Synchronous = on
		return c
	}
}
