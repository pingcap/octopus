package suite

import (
	"database/sql"
	"golang.org/x/net/context"
)

type BenchSuite interface {
	String() string
	Run(context.Context, *sql.DB) ([]*CaseResult, error)
}

type BenchCase interface {
	String() string
	Initialize(context.Context, *sql.DB) error
	Run(context.Context, *sql.DB) (*CaseResult, error)
}

type CaseMaker func() BenchCase // TODO ... func(cfg)
