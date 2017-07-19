package cat

import "github.com/pingcap/octopus/schrodinger/cluster"

type CatService struct {
	cats           map[string]*cat
	clusterManager *cluster.Manager
}

func NewCatService(manager *cluster.Manager) *CatService {
	return &CatService{
		cats:           make(map[string]*cat),
		clusterManager: manager,
	}
}

//func(s *CatService) PutCat(cat *cat) {
//}

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
