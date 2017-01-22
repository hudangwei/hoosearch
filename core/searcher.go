package core

import (
    "fmt"
    "github.com/hudangwei/hoosearch/types"
    "github.com/hudangwei/hoosearch/util"
)

/**
检索器
 */

type Searcher struct {
    pathname string
    index    map[string]*types.Index
}

/**
新建一个检索器
 */
func NewSearcher(pathname string) *Searcher {
    this := &Searcher{index: make(map[string]*types.Index), pathname: pathname}
    return this
}

/**
载入索引
 */
func (this *Searcher) LoadIndex(indexname string) error {

    indexMetaFile := fmt.Sprintf("%v/%v.json", this.pathname, indexname)
    if !util.Exist(indexMetaFile) {
        fmt.Printf("[ERROR] Index [%v] in path [%v] not exist", indexname, this.pathname)
        return fmt.Errorf("Index [%v] in path [%v] not exist", indexname, this.pathname)
    }

    idx := types.NewIndex(this.pathname, indexname)
    if idx == nil {
        fmt.Printf("[ERROR] load index [%v] fail ...", indexname)
        return fmt.Errorf("Index [%v] in path [%v] not exist", indexname, this.pathname)
    }

    this.index[indexname] = idx
    return nil
}

func (this *Searcher) Search(indexname,query string, pagenum,pagesize int) ([]map[string]string, bool, int, error) {

    if _, ok := this.index[indexname]; !ok {
        if err := this.LoadIndex(indexname); err != nil {
            return nil, false, 0, fmt.Errorf("load index error : %v ", err)
        }
    }

    docids, found := this.index[indexname].Search(query)

    if found {
        lens := len(docids)
        start := pagesize * pagenum
        end := pagesize * (pagenum+1)

        if start >= lens {
            return nil, false, 0, fmt.Errorf("page overflow")
        }
        if end >= lens {
            end = lens
        }

        res := make([]map[string]string, 0)
        for _, docid := range docids[start:end] {
            info, ok := this.index[indexname].GetDocument(docid.DocId)
            if ok {
                res = append(res, info)
            }

        }
        if len(res) > 0 {
            return res, true, len(docids), nil
        }

    }

    return nil, false, 0, nil
}