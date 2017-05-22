package backend

type TiCulster interface {
	Prepare() error
	Deploy() error
	Run() error
	Stop() error
	Reset() error
}
