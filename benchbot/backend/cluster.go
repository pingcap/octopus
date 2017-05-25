package backend

type Cluster interface {
	Prepare() error
	Deploy() error
	Run() error
	Stop() error
	Reset() error
	Destory() error
	Valid() bool
}
