package util

type DocInfoSort []DocInfo

func (a DocInfoSort) Len() int      { return len(a) }
func (a DocInfoSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocInfoSort) Less(i, j int) bool {
    if a[i] == a[j] {
        return a[i].DocId < a[j].DocId
    }
    return a[i].DocId < a[j].DocId
}