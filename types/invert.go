package types

import (
    "sync"
    "fmt"
    "strings"
    "github.com/hudangwei/hoosearch/util"
    "sort"
    "os"
    "encoding/json"
    "bytes"
    "encoding/binary"
    "bufio"
)

/**
倒排类
 */

type invert struct {
    curDocID   uint64
    segmentNum uint64
    fieldName  string
    pathname   string
    fieldType  uint64
    tempIvt     []util.TempInvert
    idx        *util.Mmap
    keys       util.KV
    wg         *sync.WaitGroup
}

/**
新建一个倒排类
 */
func newInvert(pathname, fieldname string, fieldtype uint64) *invert {
    this := &invert{
        curDocID: 0,
        segmentNum: 0,
        pathname: pathname,
        fieldName: fieldname,
        fieldType: fieldtype,
        tempIvt: make([]util.TempInvert, 0),
        idx: nil,
        keys: nil,
        wg: new(sync.WaitGroup)}

    idxfilename := fmt.Sprintf("%v%v.idx", pathname, fieldname)
    dicfilename := fmt.Sprintf("%v%v.dic", pathname, fieldname)

    if util.Exist(idxfilename) && util.Exist(dicfilename) {
        this.keys = util.NewHashTable()
        this.keys.Load(dicfilename)
        fmt.Printf("[INFO] Read %v success", dicfilename)
        this.idx, _ = util.NewMmap(idxfilename, util.MODE_APPEND)
        fmt.Printf("[INFO] Read %v success", idxfilename)
    }

    return this
}

func (this *invert) searchTerm(term string) ([]util.DocInfo, bool) {

    nospaceterm := strings.TrimSpace(term)
    if len(nospaceterm) == 0 {
        return nil, false
    }

    if offset, ok := this.keys.Get(nospaceterm); ok {

        lens := this.idx.ReadInt64(int64(offset))
        res := this.idx.ReadDocIdsArry(uint64(offset+8), uint64(lens))
        return res, true

    }
    return nil, false

}

/**
增加doc到倒排中
 */
func (this *invert) addDocument(docid uint64, content string) error {

    //切词
    terms := util.GSegmenter.Segment(content, true)

    //写入临时数据中
    for _, term := range terms {
        nospaceterm := strings.TrimSpace(term)
        if len(nospaceterm) > 0 {
            this.tempIvt = append(this.tempIvt, util.TempInvert{DocID: docid, Term: nospaceterm})
        }
    }

    return nil
}

/**
保存k docid到磁盘
 */
func (this *invert) saveTempInvert() error {
    filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, this.segmentNum)
    this.segmentNum++
    sort.Sort(util.TempInvertTermSort(this.tempIvt))

    fout, err := os.Create(filename)
    defer fout.Close()
    if err != nil {
        fmt.Printf("[ERROR] creat [%v] error : %v", filename, err)
        return err
    }

    for _, tmpnode := range this.tempIvt {
        info_json, err := json.Marshal(tmpnode)
        if err != nil {
            fmt.Printf("[ERROR] Marshal err %v\n", tmpnode)
            return err
        }
        fout.WriteString(string(info_json) + "\n")
    }

    this.tempIvt = make([]util.TempInvert, 0)

    return nil
}

type tempMergeNode struct {
    Term   string
    DocIds []util.DocInfo
}

/**
合并磁盘上的k docid
 */
func (this *invert) mergeTempInvert() error {
    mergeChanList := make([]chan tempMergeNode, 0)
    for i := uint64(0); i < this.segmentNum; i++ {
        filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, i)
        mergeChanList = append(mergeChanList, make(chan tempMergeNode))
        this.wg.Add(1)
        go this.mapRoutine(filename, &mergeChanList[i])
    }

    this.wg.Add(1)
    filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, this.segmentNum)
    go this.reduceRoutine(filename, &mergeChanList)

    //this.Logger.Info("[INFO] Waiting [%v] Routines", this.segmentNum)
    this.wg.Wait()
    fmt.Printf("[INFO] finish [%v] Routines", this.segmentNum)

    return nil
}

/**
合并协程
 */
func (this *invert) reduceRoutine(filename string, mergeChanList *[]chan tempMergeNode) error {

    defer this.wg.Done()

    lens := len(*mergeChanList)
    maxTerm := ""
    closeCount := make([]bool, lens)
    nodes := make([]tempMergeNode, 0)
    this.keys = util.NewHashTable()

    idxFileName := fmt.Sprintf("%v%v.idx", this.pathname, this.fieldName)
    idxFd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
    if err != nil {
        return err
    }
    defer idxFd.Close()

    fmt.Printf("[INFO] reduce [%v] indexs start ... ", lens)

    dicFileName := fmt.Sprintf("%v%v.dic", this.pathname, this.fieldName)
    dicFd, err1 := os.OpenFile(dicFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
    if err1 != nil {
        return err1
    }
    defer dicFd.Close()

    totalOffset := uint64(0)

    //读取第一个数据
    for i, v := range *mergeChanList {
        vv, ok := <-v
        if ok {
            if maxTerm < vv.Term {
                maxTerm = vv.Term
            }
        } else {
            closeCount[i] = true
        }
        nodes = append(nodes, vv)
    }
    nextmax := ""

    //合并
    for {
        var resnode tempMergeNode
        resnode.DocIds = make([]util.DocInfo, 0)
        resnode.Term = maxTerm
        closeNum := 0
        for i, _ := range nodes {
            if maxTerm == nodes[i].Term {
                resnode.DocIds = append(resnode.DocIds, nodes[i].DocIds...)
                vv, ok := <-(*mergeChanList)[i]
                if ok {
                    nodes[i].Term = vv.Term
                    nodes[i].DocIds = vv.DocIds
                } else {
                    closeCount[i] = true
                }
            }
            if !closeCount[i] {
                if nextmax <= nodes[i].Term {
                    nextmax = nodes[i].Term
                }
                closeNum++
            }
        }
        sort.Sort(util.DocInfoSort(resnode.DocIds))

        lens := uint64(len(resnode.DocIds))
        lenBufer := make([]byte, 8)
        binary.LittleEndian.PutUint64(lenBufer, lens)

        idxFd.Write(lenBufer)
        buffer := new(bytes.Buffer)
        err = binary.Write(buffer, binary.LittleEndian, resnode.DocIds)
        if err != nil {
            fmt.Printf("[ERROR] invert --> Serialization :: Error %v", err)
            return err
        }
        idxFd.Write(buffer.Bytes())
        this.keys.Push(resnode.Term, uint64(totalOffset))

        totalOffset = totalOffset + uint64(8) + lens*util.DOCNODE_SIZE

        if closeNum == 0 {
            break
        }
        maxTerm = nextmax
        nextmax = ""
    }
    this.keys.Save(dicFileName)
    fmt.Printf("[INFO] reduce [%v] indexs finish ... ", lens)

    return nil
}

/**
map协程，读取文件，提交到channel中
 */
func (this *invert) mapRoutine(filename string, tmpmergeChan *chan tempMergeNode) error {

    defer this.wg.Done()

    datafile, err := os.Open(filename)
    if err != nil {
        fmt.Printf("[ERROR] Open File[%v] Error %v\n", filename, err)
        return err
    }
    defer datafile.Close()
    fmt.Printf("[INFO] map file[%v] index start ...", filename)
    scanner := bufio.NewScanner(datafile)
    var node tempMergeNode
    if scanner.Scan() {
        var v util.TempInvert
        content := scanner.Text()
        json.Unmarshal([]byte(content), &v)
        node.Term = v.Term
        node.DocIds = make([]util.DocInfo, 0)
        node.DocIds = append(node.DocIds, util.DocInfo{DocId: v.DocID})

    }

    for scanner.Scan() {
        var v util.TempInvert
        content := scanner.Text()
        json.Unmarshal([]byte(content), &v)
        if v.Term != node.Term {
            *tmpmergeChan <- node
            node.Term = v.Term
            node.DocIds = make([]util.DocInfo, 0)
            node.DocIds = append(node.DocIds, util.DocInfo{DocId: v.DocID})
        } else {
            node.DocIds = append(node.DocIds, util.DocInfo{DocId: v.DocID})
        }

    }
    *tmpmergeChan <- node
    close(*tmpmergeChan)
    os.Remove(filename)
    fmt.Printf("[INFO] file[%v] process finish ...", filename)
    return nil
}