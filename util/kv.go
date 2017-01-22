package util

/**
kv接口定义
hashtable为kv接口的一种实现
 */
type KV interface {
    Set(key string, value uint64) error
    Push(key string, value uint64) error
    Save(fielname string) error
    Load(filename string) error
    Get(key string) (uint64, bool)
}
