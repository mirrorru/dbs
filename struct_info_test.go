package dbs_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/mirrorru/dbs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SomeKey struct {
	ID int64 `dbs:"auto;pk"`
}
type SomeBody struct {
	Kind uint
	Name string
}

type somePrivateData struct {
	InvisibleField string
}

type SubKey struct {
	ID1 int64 `dbs:"auto;pk"`
	//ID2 int64 `dbs:"auto;pk"`
}

func (*SubKey) TableName() string {
	return subRecTableName
}

type SubBody struct {
	Kind uint
	Name string
}

type SubRec struct {
	SubKey
	SubBody
}

const subRecTableName = "SubRecTableName"

// test - мероприятия, деятельность
type SomeRec struct {
	SomeKey
	SomeBody
	somePrivateData
	Solid     SubRec // solid field info, need own Scan & Value methods for database/sql
	Inline    SubRec `dbs:"inline"` // field info exploding to subfields
	Ptr       *SubKey
	RefPtr    *SubKey   `dbs:"ref"`
	RefStruct SubKey    `dbs:"ref"`
	AuxField  time.Time `dbs:"auto"`
}

func TestStructInfo_FieldInfoList(t *testing.T) {
	t.Parallel()

	rec := SomeRec{}
	si, err := dbs.NewStructInfo(&rec)
	require.NoError(t, err)

	assert.IsType(t, reflect.TypeOf(SomeRec{}), si.Type())
	assert.Len(t, si.AllFields(), 11, "AllFields()")
	assert.Len(t, si.PKFields(), 2, "PKFields()")
	assert.Len(t, si.AutoFields(), 3, "AutoFields()")
	assert.Len(t, si.NonAutoFields(), 8, "NonAutoFields()")
	assert.Len(t, si.NonPKFields(), 9, "NonPKFields()")
}

func TestStructInfoTableName(t *testing.T) {
	t.Parallel()

	var (
		si  *dbs.StructInfo
		err error
	)

	si, err = dbs.NewStructInfo(SomeRec{})
	require.NoError(t, err)
	assert.Equal(t, si.TableName(), "some_rec")

	si, err = dbs.NewStructInfo(&SomeRec{})
	require.NoError(t, err)
	assert.Equal(t, si.TableName(), "some_rec")

	si, err = dbs.NewStructInfo(SubRec{})
	require.NoError(t, err)
	assert.Equal(t, si.TableName(), subRecTableName)

	si, err = dbs.NewStructInfo(&SubRec{})
	require.NoError(t, err)
	assert.Equal(t, si.TableName(), subRecTableName)
}

func TestStructInfo_FieldInfoList_Refs(t *testing.T) {
	t.Parallel()
	rec := &SomeRec{}
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)

	refs, err := si.AllFields().Refs(rec)
	require.NoError(t, err)
	assert.Equal(t, []any{
		&rec.ID, &rec.Kind, &rec.Name,
		&rec.Solid,
		&rec.Inline.ID1, &rec.Inline.Kind, &rec.Inline.Name,
		&rec.Ptr,
		&rec.RefPtr,
		&rec.RefStruct,
		&rec.AuxField,
	}, refs, "AllFields()")

}
