package core

import (
    "os"
    "fmt"
    "github.com/hudangwei/hoosearch/types"
    "github.com/hudangwei/hoosearch/util"
    "bufio"
    "strings"
)

/**
索引器
 */

type Indexer struct {
    pathname string
    indexs   map[string]*types.Index
}

/**
新建索引器
 */
func NewIndexer(pathname string) *Indexer {
    this := &Indexer{pathname: pathname, indexs: make(map[string]*types.Index)}
    if pathname == "./" {
        fmt.Printf("[ERROR] pathname can not use [%v]", pathname)
        return nil
    }
    if util.Exist(pathname) {
        fmt.Printf("[INFO] pathname [%v]", pathname)
        os.RemoveAll(pathname)
    }

    os.MkdirAll(pathname, os.ModeDir|os.ModePerm)

    return this
}

/**
添加一个索引
 */
func (this *Indexer) AddIndex(indexname string, fields map[string]uint64) error {

    if _, ok := this.indexs[indexname]; ok {
        fmt.Printf("[ERROR] index [%v] is exist", indexname)
        return fmt.Errorf("[ERROR] index [%v] is exist", indexname)
    }

    this.indexs[indexname] = types.NewIndex(this.pathname, indexname)
    if this.indexs[indexname] == nil {
        return fmt.Errorf("[ERROR] create index [%v] error", indexname)
    }
    return this.indexs[indexname].MappingFields(fields)
}


func (this *Indexer) LoadData(indexname, datafilename, fieldType string, fieldnames []string) error {

    datafile, err := os.Open(datafilename)
    if err != nil {
        fmt.Printf("[ERROR] Open File[%v] Error %v\n", datafilename, err)
        return err
    }
    defer datafile.Close()
    scanner := bufio.NewScanner(datafile)
    scanner.Buffer(make([]byte,1024*1024),1024*1024)
    document := make(map[string]string)
    count := 0
    for scanner.Scan() {
        if fieldType == "text" {
            text := scanner.Text()
            subtexts := strings.Split(text, "\t")
            sublen := len(subtexts)
            for i, t := range fieldnames {
                if i >= sublen {
                    document[t] = ""
                } else {
                    document[t] = subtexts[i]
                }

            }
            this.indexs[indexname].AddDocument(document)
        }
        count++
        if count%10000 == 0 {
            fmt.Printf("[INFO] Process %v Documents", count)
        }

    }
    fmt.Printf("[ERROR] scanner[%v] error : %v",count,scanner.Err())

    return this.indexs[indexname].Serialization()
}