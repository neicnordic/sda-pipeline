package storage

// S3Conf stores information about the S3 backend
type S3Conf struct {
	URL        string
	Port       string
	AccessKey  string
	SecretKey  string
	Bucket     string
	Chunksize  int
	Cacert     string
}

// PosixConf stores information about the posix backend
type PosixConf struct {
	Location   string
	Mode       int
	User       string
	Separator  string
}
