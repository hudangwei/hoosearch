package util

type TempInvertTermSort []TempInvert

func (a TempInvertTermSort) Len() int      { return len(a) }
func (a TempInvertTermSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a TempInvertTermSort) Less(i, j int) bool {
    if a[i] == a[j] {
        return a[i].Term > a[j].Term
    }
    return a[i].Term > a[j].Term
}