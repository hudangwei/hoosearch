package util

import (
    "os"
    "encoding/json"
    "fmt"
    "io/ioutil"
)

/**
判断文件是否存在
 */
func Exist(filename string) bool {
    _, err := os.Stat(filename)
    return err == nil || os.IsExist(err)
}

/**
写入json文件
 */
func WriteToJson(data interface{}, file_name string) error {

    info_json, err := json.Marshal(data)
    if err != nil {
        fmt.Printf("Marshal err %v\n", file_name)
        return err
    }

    fout, err := os.Create(file_name)
    defer fout.Close()
    if err != nil {

        return err
    }
    fout.Write(info_json)
    return nil
}

/**
读取json文件
 */
func ReadFromJson(file_name string) ([]byte, error) {

    fin, err := os.Open(file_name)
    defer fin.Close()
    if err != nil {
        return nil, err
    }

    buffer, err := ioutil.ReadAll(fin)
    if err != nil {
        return nil, err
    }
    return buffer, nil

}
/**
二路求交集
 */
func Interaction(a []DocInfo, b []DocInfo) ([]DocInfo, bool) {

    if a == nil || b == nil {
        return nil, false
    }

    lena := len(a)
    lenb := len(b)
    var c []DocInfo
    lenc := 0
    if lena < lenb {
        c = make([]DocInfo, lena)
    } else {
        c = make([]DocInfo, lenb)
    }
    ia := 0
    ib := 0
    for ia < lena && ib < lenb {
        if a[ia].DocId == b[ib].DocId {
            c[lenc] = a[ia]
            lenc++
            ia++
            ib++
            continue
        }
        if a[ia].DocId < b[ib].DocId {
            ia++
        } else {
            ib++
        }
    }

    if len(c) == 0 {
        return nil, false
    } else {
        return c[:lenc], true
    }

}

func Merge(a []DocInfo, b []DocInfo) ([]DocInfo, bool) {
    lena := len(a)
    lenb := len(b)
    lenc := 0
    c := make([]DocInfo, lena+lenb)
    ia := 0
    ib := 0
    if lena == 0 && lenb == 0 {
        return nil, false
    }

    for ia < lena && ib < lenb {

        if a[ia] == b[ib] {
            c[lenc] = a[ia]
            lenc++
            ia++
            ib++
            continue
        }

        if a[ia].DocId < b[ib].DocId {
            c[lenc] = a[ia]
            lenc++
            ia++
        } else {
            c[lenc] = b[ib]
            lenc++
            ib++
        }
    }

    if ia < lena {
        for ; ia < lena; ia++ {
            c[lenc] = a[ia]
            lenc++
        }

    } else {
        for ; ib < lenb; ib++ {
            c[lenc] = b[ib]
            lenc++
        }
    }

    return c[:lenc], true

}
