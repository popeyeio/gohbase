package instance

type Instance interface {
	GetAddr() string
	GetWeight() int
	GetIDC() string
	GetCluster() string
}
