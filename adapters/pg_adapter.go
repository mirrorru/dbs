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

func WriteFieldInfoListNames(writer io.StringWriter, list dbs.FieldInfoList, separator string) {
	for idx := range list {
		if idx > 0 {
			_, _ = writer.WriteString(separator)
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

func WriteFieldInfoListEQs(writer io.StringWriter, list dbs.FieldInfoList, startIdx int, separator string) {
	for idx := range list {
		if idx > 0 {
			_, _ = writer.WriteString(separator)
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
		sb.WriteString("INSERT INTO ")
		sb.WriteString(info.TableName())
		sb.WriteString(" (")
		WriteFieldInfoListNames(&sb, info.NonAutoFields(), ", ")
		sb.WriteString(") VALUES ($")
		WriteFieldInfoListIdxs(&sb, info.NonAutoFields(), 1, ", $")
		sb.WriteString(") RETUNING (")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		sb.WriteString(");")

		return sb.String()
	})
}

func (PGAdapter) SelectOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindSelectOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		pkFields := info.PKFields()
		sb.Grow(30 + len(allFields)*2*DefaultFieldNameLength + len(pkFields)*DefaultFieldNameLength)

		sb.WriteString("SELECT ")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		sb.WriteString(" FROM ")
		sb.WriteString(info.TableName())
		sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, pkFields, 1, " AND ")
		sb.WriteString(" LIMIT 1;")

		return sb.String()
	})
}

func (PGAdapter) UpdateOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindUpdateOne}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		nonPkFields := info.NonPKFields()
		sb.Grow(40 + len(allFields)*3*DefaultFieldNameLength)
		sb.WriteString("UPDATE ")
		sb.WriteString(info.TableName())
		sb.WriteString(" SET ")
		WriteFieldInfoListEQs(&sb, nonPkFields, 1, ", ")
		sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, info.PKFields(), len(nonPkFields)+1, " AND ")
		sb.WriteString(" RETUNING (")
		WriteFieldInfoListNames(&sb, allFields, ", ")
		sb.WriteString(");")

		return sb.String()
	})
}

func (PGAdapter) DeleteOneQuery(info *dbs.StructInfo) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindDeleteOne}, func() string {
		var sb strings.Builder

		pkFields := info.PKFields()
		sb.Grow(20 + len(pkFields)*2*DefaultFieldNameLength)
		sb.WriteString("DELETE FROM ")
		sb.WriteString(info.TableName())
		sb.WriteString(" WHERE ")
		WriteFieldInfoListEQs(&sb, info.PKFields(), 1, " AND ")
		sb.WriteString(";")

		return sb.String()
	})
}
