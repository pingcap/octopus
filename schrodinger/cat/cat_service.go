package cat

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/schrodinger/cluster"
	"github.com/pingcap/octopus/schrodinger/config"
)

type CatService struct {
	config         *config.Config
	cats           map[string]*Cat
	clusterManager *cluster.Manager
}

func NewCatService(manager *cluster.Manager, conf *config.Config) *CatService {
	cs := &CatService{
		cats:           make(map[string]*Cat),
		config:         conf,
		clusterManager: manager,
	}
	return cs
}

func (s *CatService) PutCat(c *Cat) error {
	if _, ok := s.cats[c.Name]; ok {
		log.Warnf("cat already exists: [%s]", c.Name)
		return errors.New(fmt.Sprintf("[%s] cat already exists", c.Name))
	}
	c.Status = CATStatus(STOP)
	s.cats[c.Name] = c
	return nil
}

func (s *CatService) RunCat(name string) error {
	// TODO: to run cat
	return nil
}

func (s *CatService) IsExist(name string) bool {
	if _, ok := s.cats[name]; ok {
		return true
	}
	return false
}
