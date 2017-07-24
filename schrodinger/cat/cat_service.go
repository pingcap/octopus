package cat

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/schrodinger/cat"
	"github.com/pingcap/octopus/schrodinger/cluster"
)

type CatService struct {
	cats           map[string]*Cat
	clusterManager *cluster.Manager
}

func NewCatService(manager *cluster.Manager) *CatService {
	return &CatService{
		cats:           make(map[string]*Cat),
		clusterManager: manager,
	}
}

func (s *CatService) PutCat(c *Cat) error {
	if _, ok := s.cats[c.Name]; ok {
		log.Warn("cat already exists: [%s]", c.Name)
		return errors.New(fmt.Sprintf("cat already exists: [%s]", c.Name))
	}
	c.Status = cat.STOP
	s.cats[c.Name] = c
	return nil
}

//func(s *CatService) DeleteCat(name string) {
//}

//func(s *CatService) StartCat(name string) {
//}

//func(s *CatService) StopCat(name string) {
//}

//func (s *CatService) CloseCatService() {
//}

//func (s *CatService) initCatService() {
//}
