package adapters_test

import (
	"testing"
	"time"

	"github.com/mirrorru/dbs"
	"github.com/mirrorru/dbs/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestKey struct {
	ID int64 `dbs:"auto;pk"`
}
type TestBody struct {
	Kind uint
	Name string
}

type testPrivateData struct {
	InvisibleField string
}

// test - мероприятия, деятельность
type TestRec struct {
	TestKey
	TestBody
	testPrivateData
	AuxField time.Time
}

func testRecAllRefs(rec *TestRec) []any {
	return []any{&rec.TestKey.ID, &rec.TestBody.Kind, &rec.TestBody.Name, &rec.AuxField}
}

func testRecPKRefs(rec *TestRec) []any {
	return []any{&rec.TestKey.ID}
}

func testRecNonPKRefs(rec *TestRec) []any {
	return []any{&rec.TestBody.Kind, &rec.TestBody.Name, &rec.AuxField}
}

func TestInsertOneArgs(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.InsertOneArgs(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecNonPKRefs(&rec), refs)
}

func TestInsertOneReceivers(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.InsertOneReceivers(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecAllRefs(&rec), refs)
}

func TestSelectOneArgs(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.SelectOneArgs(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecPKRefs(&rec), refs)
}

func TestSelectOneReceivers(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.SelectOneReceivers(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecAllRefs(&rec), refs)
}

func TestUpdateOneArgs(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.UpdateOneArgs(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, append(testRecNonPKRefs(&rec), testRecPKRefs(&rec)...), refs)
}

func TestUpdateOneReceivers(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.UpdateOneReceivers(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecAllRefs(&rec), refs)
}

func TestDeleteOneArgs(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.DeleteOneArgs(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecPKRefs(&rec), refs)
}

func TestDeleteOneReceivers(t *testing.T) {
	t.Parallel()
	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)
	refs, err := adapters.DeleteOneReceivers(si, &rec)
	require.NoError(t, err)
	assert.Equal(t, testRecAllRefs(&rec), refs)
}
