package adapters

import (
	"io"
	"reflect"
	"strconv"

	"github.com/mirrorru/dbs"
	"github.com/mirrorru/dot"
)

// DefaultFieldNameLength ожидаемая длина имени поля, используется для выделения памяти при создании запросов
var DefaultFieldNameLength = 20

type queryKind byte

const (
	queryKindInsertOne queryKind = iota
	queryKindSelectOne
	queryKindUpdateOne
	queryKindDeleteOne
)

type queryCacheKey struct {
	QueryOptions

	Type reflect.Type
	Kind queryKind
}

type QueryOptions struct {
	WithTotals bool
	WithAlias  string
}

var queryCache = dot.SyncStore[queryCacheKey, string]{}

func WriteFieldInfoListNames(writer io.StringWriter, list dbs.FieldInfoList, sepaPrefix string) {
	for idx := range list {
		if idx > 0 {
			_, _ = writer.WriteString(sepaPrefix)
		}
		_, _ = writer.WriteString(list[idx].Name)
	}
}

func WriteFieldInfoListIdxs(writer io.StringWriter, list dbs.FieldInfoList, startIdx int, separator string) {
	for idx := range list {
		if idx > 0 {
			_, _ = writer.WriteString(separator)
		}
		_, _ = writer.WriteString(strconv.Itoa(startIdx))
		startIdx++
	}
}

func WriteFieldInfoListEQs(writer io.StringWriter, list dbs.FieldInfoList, startIdx int, sepaPrefix string) {
	for idx := range list {
		if idx > 0 {
			_, _ = writer.WriteString(sepaPrefix)
		}
		_, _ = writer.WriteString(list[idx].Name)
		_, _ = writer.WriteString("=$")
		_, _ = writer.WriteString(strconv.Itoa(startIdx))
		startIdx++
	}
}

func InsertOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.NonAutoFields().Refs(src)
}

func InsertOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}

func SelectOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.PKFields().Refs(src)
}

func SelectOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}

func UpdateOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	npk, err := info.NonPKFields().Refs(src)
	if err != nil {
		return nil, err
	}
	pks, err := info.PKFields().Refs(src)
	if err != nil {
		return nil, err
	}
	return append(npk, pks...), nil
}

func UpdateOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}

func DeleteOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.PKFields().Refs(src)
}

func DeleteOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}
