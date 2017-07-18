package cat

type CatService struct {
	Cats map[string]*Cat
}

func NewCatService() *CatService{
	return &CatService{
		Cats : make(map[string]*Cat)
	}
}

//func(cs *CatService) PutCat(cat *Cat) {
//}

//func(Cs *CatService) DeleteCat(name string) {
//}

//func(cs *CatService) StartCat(name string) {
//}

//func(cs *CatService) StopCat(name string) {
//}
