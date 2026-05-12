package credis

type Config struct {
	Dir       string
	Host      string
	ReplicaOf string
	Port      int
}

func NewCfg(flgs *Flags) *Config {
	return &Config{
		Dir:       flgs.Dir,
		Host:      flgs.Host,
		ReplicaOf: flgs.ReplicaOf,
		Port:      flgs.Port,
	}
}
