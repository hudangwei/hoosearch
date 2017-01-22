package main

import (
    "github.com/hudangwei/hoosearch/util"
    "github.com/hudangwei/hoosearch/core"
    "fmt"
)

/**
全局
 */
var GIndexer *core.Indexer
var GSearcher *core.Searcher

func main()  {
    //设置分词
    util.GSegmenter = util.NewSegmenter("./data/dictionary.txt")

    GSearcher = core.NewSearcher("./index")
    GIndexer = core.NewIndexer("./index")

    //索引名
    indexName := "api"

    fields := make(map[string]string)
    fields["title"] = "string"
    fields["keyword"] = "string"
    fields["url"] = "onlyshow"
    fields["hot"] = "number"


    fieldMap := make(map[string]uint64)
    for k, v := range fields {
        switch v {
        case "string":
            fieldMap[k] = util.TString
        case "number":
            fieldMap[k] = util.TNumber
        case "onlyshow":
            fieldMap[k] = util.TStore
        default:
        }
    }
    GIndexer.AddIndex(indexName, fieldMap)

    //数据源文件
    dataFilePath := "./sp.txt"

    fileType := "text"

    contentDesc := []string{"title","keyword","url","hot"}
    GIndexer.LoadData(indexName,dataFilePath,fileType,contentDesc)

    GSearcher.LoadIndex(indexName)

    res, found, count, err := GSearcher.Search(indexName,"小米",0,2)
    if err != nil {
        fmt.Println(err)
    }
    if found {
        fmt.Println("count:",count)
        fmt.Println(res)
    }
}
