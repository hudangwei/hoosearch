package util

import "fmt"

/**
分词器封装
 */
type Segmenter struct {
    dictionary  string
    fssegmenter *FSSegmenter
}

/**
全局分词器
 */
var GSegmenter *Segmenter

/**
加载分词字典
 */
func NewSegmenter(dic_name string) *Segmenter {
    this := &Segmenter{dictionary: dic_name}
    this.fssegmenter = NewFSSegmenter(dic_name)
    if this == nil {
        fmt.Errorf("ERROR segment is nil")
        return nil
    }
    return this
}

/**
切词
 */
func (this *Segmenter) Segment(content string, search_mode bool) []string {

    terms, _ := this.fssegmenter.Segment(content, search_mode)

    //去重
    termmap := make(map[string]bool)
    res := make([]string, 0)
    for _, term := range terms {
        if _, ok := termmap[term]; !ok {
            termmap[term] = true
            res = append(res, term)
        }
    }

    return res
}