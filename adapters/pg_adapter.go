package adapters

import (
	"strings"

	"github.com/mirrorru/dbs"
)

type PGAdapter struct {
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
		_, _ = sb.WriteString(") RETURNING ")
		WriteFieldInfoListNames(&sb, allFields, ", ")

		return sb.String()
	})
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

func (PGAdapter) SelectManyQuery(info *dbs.StructInfo, opts QueryOptions) string {
	return queryCache.GetOrPut(queryCacheKey{Type: info.Type(), Kind: queryKindSelectOne, QueryOptions: opts}, func() string {
		var sb strings.Builder

		allFields := info.AllFields()
		sb.Grow(30 + (len(allFields)+1)*2*(DefaultFieldNameLength+len(opts.WithAlias)))

		prefix := ", "
		_, _ = sb.WriteString("SELECT ")
		if len(opts.WithAlias) > 0 {
			_, _ = sb.WriteString(opts.WithAlias)
			_, _ = sb.WriteString(".")
			prefix += opts.WithAlias + "."
		}
		WriteFieldInfoListNames(&sb, allFields, prefix)
		if opts.WithTotals {
			_, _ = sb.WriteString(", COUNT(")
			if len(opts.WithAlias) > 0 {
				_, _ = sb.WriteString(opts.WithAlias)
				_, _ = sb.WriteString(".")
			}
			_, _ = sb.WriteString("*) OVER()")
		}
		_, _ = sb.WriteString(" FROM ")
		_, _ = sb.WriteString(info.TableName())
		if len(opts.WithAlias) > 0 {
			_, _ = sb.WriteString(" ")
			_, _ = sb.WriteString(opts.WithAlias)
		}

		return sb.String()
	})
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
		_, _ = sb.WriteString(" RETURNING ")
		WriteFieldInfoListNames(&sb, allFields, ", ")

		return sb.String()
	})
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
		_, _ = sb.WriteString(" RETURNING ")
		WriteFieldInfoListNames(&sb, allFields, ", ")

		return sb.String()
	})
}
