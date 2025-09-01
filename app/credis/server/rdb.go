package server

func WithRDBDir(dir string) ConfigOption {
	return func(r *config) {
		r.rdbDir = dir
	}
}

func WithRDBFileName(fileName string) ConfigOption {
	return func(r *config) {
		r.rdbFileName = fileName
	}
}
