package suite

type CaseResult struct {
	Name string      `json:"name"`
	Stat *StatResult `json:"stat"`
}

func NewCaseResult(name string, stat *StatResult) *CaseResult {
	return &CaseResult{
		Name: name,
		Stat: stat,
	}
}
