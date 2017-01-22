package types

import (
    "github.com/hudangwei/hoosearch/util"
    "fmt"
    "strconv"
)

/**
正排类
 */


type profile struct {
    maxDocID  uint64
    fieldName string
    pathName  string
    fieldType uint64
    pfl       *util.Mmap
    detail    *util.Mmap
}

/**
新建一个正排类
 */
func newProfile(pathname,fieldname string, fieldtype uint64) *profile {
    this := &profile{
        pathName:pathname,
        fieldName:fieldname,
        fieldType: fieldtype,
        maxDocID:0}

    //打开数据文件
    pflfilename := fmt.Sprintf("%v%v.pfl", pathname, fieldname)
    dtlfilename := fmt.Sprintf("%v%v.dtl", pathname, fieldname)

    if fieldtype == util.TString || fieldtype == util.TStore {
        if util.Exist(pflfilename) && util.Exist(dtlfilename) {
            this.pfl, _ = util.NewMmap(pflfilename, util.MODE_APPEND)
            this.detail, _ = util.NewMmap(dtlfilename, util.MODE_APPEND)

        } else {
            this.pfl, _ = util.NewMmap(pflfilename, util.MODE_CREATE)
            this.detail, _ = util.NewMmap(dtlfilename, util.MODE_CREATE)
        }

    }

    if fieldtype == util.TNumber {
        if util.Exist(pflfilename) {
            this.pfl, _ = util.NewMmap(pflfilename, util.MODE_APPEND)
        } else {
            this.pfl, _ = util.NewMmap(pflfilename, util.MODE_CREATE)
        }
        this.detail = nil
    }

    return this
}

/**
增加doc内容到正排中
 */
func (this *profile) addDocument(docid uint64, content string) error {

    //写入数据文件中
    if this.fieldType == util.TString || this.fieldType == util.TStore {
        offset := uint64(this.detail.GetPointer())
        this.pfl.AppendUInt64(offset)
        this.detail.AppendStringWithLen(content)
        return nil
    }

    value, err := strconv.ParseUint(content, 0, 0)
    if err != nil {
        value = 0
    }
    this.pfl.AppendUInt64(value)
    return nil
}

func (this *profile) filted(docid, value, typ uint64) bool {

    offset := this.pfl.ReadUInt64(docid * 8)

    switch typ {
    case util.EQ:
        return offset == value
    case util.UNEQ:
        return offset != value
    case util.OVER:
        return offset > value
    default:
        return false
    }
}

/**
根据docid获取详情
 */
func (this *profile) getDetail(docid uint64) interface{} {

    offset := this.pfl.ReadUInt64(docid * 8)

    if this.fieldType == util.TString || this.fieldType == util.TStore {
        return this.detail.ReadStringWithLen(offset)
    }

    return offset
}

/**
同步数据到磁盘
 */
func (this *profile) sync() error {

    if this.fieldType == util.TString || this.fieldType == util.TStore {
        this.detail.Sync()
    }
    return this.pfl.Sync()

}