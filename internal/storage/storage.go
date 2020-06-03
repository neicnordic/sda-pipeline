package storage

// S3Conf stores information about the S3 backend
type S3Conf struct {
	URL       string
	Port      int
	AccessKey string
	SecretKey string
	Bucket    string
	Chunksize int
	Cacert    string
}

// PosixConf stores information about the posix backend
type PosixConf struct {
	Location string
	Mode     int
	UID      int
	GID      int
}
