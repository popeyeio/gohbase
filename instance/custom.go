package instance

type customInstance struct {
	addr string
	weight int
	idc string
	cluster string
}

var _ Instance = (*customInstance)(nil)

func NewCustomInstance(addr string) Instance {
	return &customInstance{
		addr: addr,
	}
}

func (i customInstance) GetAddr() string {
	return i.addr
}

func (i customInstance) GetWeight() int {
	return i.weight
}

func (i customInstance) GetIDC() string {
	return i.idc
}

func (i customInstance) GetCluster() string {
	return i.cluster
}
