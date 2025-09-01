package server

type SectionInfo map[string]string

type ServerInfo map[string]SectionInfo

func NewInfo() ServerInfo {
	return ServerInfo{
		"replication": make(SectionInfo),
	}
}

func (info *ServerInfo) Get(section string, key string) string {
	return (*info)[section][key]
}

func (info *ServerInfo) Section(section string) map[string]string {
	return (*info)[section]
}

func (info *ServerInfo) set(section string, key string, value string) {
	(*info)[section][key] = value
}
