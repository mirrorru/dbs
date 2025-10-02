package adapters

import (
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/mirrorru/dbs"
	"github.com/mirrorru/dot"
)

// DefaultFieldNameLength ожидаемая длина имени поля, используется для выделения памяти при создании запросов
var DefaultFieldNameLength = 20

type PGAdapter struct {
}

type queryKind byte

const (
	queryKindInsertOne queryKind = iota
	queryKindSelectOne
	queryKindUpdateOne
	queryKindDeleteOne
)

type queryCacheKey struct {
	Type reflect.Type
	Kind queryKind
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

func (PGAdapter) CanReturnValuesInDML() bool {
	return true
}

func (PGAdapter) InsertOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindInsertOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		sb.Grow(50 + len(allFields)*3*DefaultFieldNameLength)
		_, _ = sb.WriteString("INSERT INTO ")
		_, _ = sb.WriteString(info.TableName())
		_, _ = sb.WriteString(" (")
		WriteFieldInfoListNames(&sb, info.NonAutoFields(), ", ")
		_, _ = sb.WriteString(") VALUES ($")
		WriteFieldInfoListIdxs(&sb, info.NonAutoFields(), 1, ", $")
		_, _ = sb.WriteString(") RETURNING (")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		_, _ = sb.WriteString(");")

		return sb.String()
	})
}

func InsertOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.NonAutoFields().Refs(src)
}

func InsertOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}

func (PGAdapter) SelectOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindSelectOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		pkFields := info.PKFields()
		sb.Grow(30 + len(allFields)*2*DefaultFieldNameLength + len(pkFields)*DefaultFieldNameLength)

		_, _ = sb.WriteString("SELECT ")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		_, _ = sb.WriteString(" FROM ")
		_, _ = sb.WriteString(info.TableName())
		_, _ = sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, pkFields, 1, " AND ")
		_, _ = sb.WriteString(" LIMIT 1;")

		return sb.String()
	})
}

func (PGAdapter) SelectManyQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindSelectOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		sb.Grow(30 + len(allFields)*2*DefaultFieldNameLength)

		_, _ = sb.WriteString("SELECT ")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		_, _ = sb.WriteString(" FROM ")
		_, _ = sb.WriteString(info.TableName())

		return sb.String()
	})
}

func SelectOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.PKFields().Refs(src)
}

func SelectOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}

func (PGAdapter) UpdateOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindUpdateOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		nonPkFields := info.NonPKFields()
		sb.Grow(40 + len(allFields)*3*DefaultFieldNameLength)
		_, _ = sb.WriteString("UPDATE ")
		_, _ = sb.WriteString(info.TableName())
		_, _ = sb.WriteString(" SET ")
		WriteFieldInfoListEQs(&sb, nonPkFields, 1, ", ")
		_, _ = sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, info.PKFields(), len(nonPkFields)+1, " AND ")
		_, _ = sb.WriteString(" RETURNING (")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		_, _ = sb.WriteString(");")

		return sb.String()
	})
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

func (PGAdapter) DeleteOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindDeleteOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		pkFields := info.PKFields()
		sb.Grow(20 + len(pkFields)*2*DefaultFieldNameLength)
		_, _ = sb.WriteString("DELETE FROM ")
		_, _ = sb.WriteString(info.TableName())
		_, _ = sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, info.PKFields(), 1, " AND ")
		_, _ = sb.WriteString(" RETURNING (")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		_, _ = sb.WriteString(");")

		return sb.String()
	})
}

func DeleteOneArgs[T any](info *dbs.StructInfo, src *T) ([]any, error) {
	return info.PKFields().Refs(src)
}

func DeleteOneReceivers[T any](info *dbs.StructInfo, dest *T) ([]any, error) {
	return info.AllFields().Refs(dest)
}
