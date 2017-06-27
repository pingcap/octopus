package suite

type CaseResult struct {
	Name    string       `json:"name"`
	Summary *StatIndex   `json:"summary"`
	Stages  []*StatIndex `json:"stages"`
}

func NewCaseResult(name string) *CaseResult {
	return &CaseResult{
		Name:   name,
		Stages: make([]*StatIndex, 0),
	}
}
