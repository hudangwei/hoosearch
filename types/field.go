package types

import (
	"fmt"
    "github.com/hudangwei/hoosearch/util"
    "encoding/json"
)

/**
字段类
*/

type field struct {
	MaxDocID uint64 `json:"maxdocid"`
	Name     string `json:"name"`
	Type     uint64 `json:"type"`
	pathname string

	pfl *profile
	ivt *invert
}

/**
新建一个字段
*/
func newField(pathname, fieldname string, ftype uint64) *field {

	this := &field{
		pathname: pathname,
		Name:     fieldname,
		Type:     ftype,
		MaxDocID: 0,
		pfl:      nil,
		ivt:      nil}

    fieldMetaFilename := fmt.Sprintf("%v%v.json", pathname, fieldname)
    if util.Exist(fieldMetaFilename) {
        if bvar, err := util.ReadFromJson(fieldMetaFilename); err == nil {
            if jerr := json.Unmarshal(bvar, this); jerr != nil {
                return nil
            }
        } else {
            return nil
        }
    }
    this.pfl = newProfile(pathname, fieldname, ftype)
    if ftype == util.TString {
        this.ivt = newInvert(pathname, fieldname, ftype)
    }

    return this
}

/**
增加doc
*/
func (this *field) addDocument(docid uint64, content string) error {

	if docid != this.MaxDocID {
		return fmt.Errorf("docid error , max docid is [%v]", this.MaxDocID)
	}

    if err := this.pfl.addDocument(docid, content); err != nil {
        fmt.Errorf("[ERROR] profile add document error %v", err)
        return err
    }
    if this.ivt != nil {
        if err := this.ivt.addDocument(docid, content); err != nil {
            fmt.Errorf("[ERROR] invert add document error %v", err)
            return err
        }
    }

    this.MaxDocID++

    if this.MaxDocID%util.IVT_SYNC_NUMBER == 0 {
        if this.ivt != nil {
            this.ivt.saveTempInvert()
        }
    }

    return nil
}

/**
序列化字段的倒排和正排到磁盘
 */
func (this *field) serialization() error {

    if this.ivt != nil {
        if err := this.ivt.saveTempInvert(); err != nil {
            fmt.Printf("[ERROR] save invert error  %v", err)
            return err
        }
        if err := this.ivt.mergeTempInvert(); err != nil {
            fmt.Printf("[ERROR] merge invert error  %v", err)
            return err
        }
    }
    if err := this.pfl.sync(); err != nil {
        fmt.Printf("[ERROR] sync profile error  %v", err)
        return err
    }
    fieldMetaFilename := fmt.Sprintf("%v%v.json", this.pathname, this.Name)
    if err := util.WriteToJson(this, fieldMetaFilename); err != nil {
        fmt.Printf("[ERROR] save field json error  %v", err)
        return err
    }

    return nil
}


func (this *field) searchTerm(term string) ([]util.DocInfo, bool) {

    if this.ivt != nil {
        return this.ivt.searchTerm(term)
    }
    return nil, false
}

func (this *field) getDetail(docid uint64) (string, uint64, bool) {

    if this.pfl != nil && docid < this.MaxDocID {
        res := this.pfl.getDetail(docid)
        if this.Type == util.TString || this.Type == util.TStore {
            return fmt.Sprintf("%s", res), 0, true
        }
        resnum, _ := res.(uint64)
        return fmt.Sprintf("%v", res), resnum, true
    }
    return "", 0, false
}