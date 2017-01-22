package types

import (
    "fmt"
    "github.com/hudangwei/hoosearch/util"
    "encoding/json"
    "strings"
)

/**
索引类
 */

type Index struct {
    Name       string `json:"indexname"`
    MaxDocID   uint64 `json:"maxdocid"`
    pathname   string
    FieldInfos map[string]uint64 `json:"Fields"`
    fields     map[string]*field
}

/**
新建索引
 */
func NewIndex(pathname, indexname string) *Index {

    this := &Index{
        Name: indexname,
        pathname: pathname,
        FieldInfos: nil,
        fields: make(map[string]*field)}

    indexMetaFile := fmt.Sprintf("%v/%v.json", pathname, indexname)
    if util.Exist(indexMetaFile) {
        if bvar, err := util.ReadFromJson(indexMetaFile); err == nil {
            if jerr := json.Unmarshal(bvar, this); jerr != nil {
                return nil
            }
        } else {
            return nil
        }
        fieldpath := fmt.Sprintf("%v/%v_", pathname, indexname)
        for fname, ftype := range this.FieldInfos {
            field := newField(fieldpath, fname, ftype)
            this.fields[fname] = field
        }
    }
    return this
}

func (this *Index) MappingFields(fieldinfos map[string]uint64) error {

    if this.FieldInfos == nil {
        this.FieldInfos = fieldinfos
        fieldpath := fmt.Sprintf("%v/%v_", this.pathname, this.Name)
        for fname, ftype := range this.FieldInfos {
            field := newField(fieldpath, fname, ftype)
            this.fields[fname] = field
        }
        fmt.Printf("[INFO] fields %v", this.FieldInfos)
        return nil
    }

    return fmt.Errorf("fields is exist")
}

/**
增加文档
 */
func (this *Index) AddDocument(document map[string]string) error {

    docid := this.MaxDocID
    this.MaxDocID++
    if this.FieldInfos == nil {
        return fmt.Errorf("no fields")
    }

    for k, field := range this.fields {
        if _, ok := document[k]; !ok {
            document[k] = ""
        }

        if err := field.addDocument(docid, document[k]); err != nil {
            fmt.Printf("[ERROR] add document error %v", err)
            this.MaxDocID--
            return err
        }
    }

    return nil
}

/**
序列化索引到磁盘
 */
func (this *Index) Serialization() error {

    if this.FieldInfos == nil {
        return fmt.Errorf("no fields")
    }

    for _, field := range this.fields {
        if err := field.serialization(); err != nil {
            fmt.Printf("[ERROR] add document error %v", err)
            return err
        }
    }

    indexMetaFile := fmt.Sprintf("%v/%v.json", this.pathname, this.Name)
    if err := util.WriteToJson(this, indexMetaFile); err != nil {
        fmt.Printf("[ERROR] save field json error  %v", err)
        return err
    }

    return nil
}

func (this *Index) Search(query string) ([]util.DocInfo, bool) {
    terms := util.GSegmenter.Segment(query, false)
    resdocids := make([]util.DocInfo, 0)
    flag := false
    //term搜索
    for _, term := range terms {
        subdocids := make([]util.DocInfo, 0)
        for k, v := range this.FieldInfos {
            if v == util.TString {
                fielddocids, ok := this.SearchTerm(term, k)
                if ok {
                    subdocids, _ = util.Merge(subdocids, fielddocids)
                }

            }
        }
        if !flag {
            resdocids = subdocids
            flag = true
        } else {
            resdocids, _ = util.Interaction(resdocids, subdocids)
        }
    }

    if len(resdocids) == 0 {
        return nil, false
    }
    return resdocids, true
}

func (this *Index) SearchTerm(term, field string) ([]util.DocInfo, bool) {

    //写入临时数据中
    nospaceterm := strings.TrimSpace(term)
    if len(nospaceterm) > 0 {
        return this.fields[field].searchTerm(nospaceterm)
    }
    return nil, false
}

func (this *Index) GetDocument(docid uint64) (map[string]string, bool) {

    if docid >= this.MaxDocID {
        return nil, false
    }
    document := make(map[string]string)

    for fname, field := range this.fields {
        v, _, ok := field.getDetail(docid)
        if ok {
            document[fname] = v
        } else {
            document[fname] = ""
        }
    }

    return document, true
}