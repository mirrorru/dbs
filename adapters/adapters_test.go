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
	return []any{&rec.ID, &rec.Kind, &rec.Name, &rec.AuxField}
}

func testRecPKRefs(rec *TestRec) []any {
	return []any{&rec.ID}
}

func testRecNonPKRefs(rec *TestRec) []any {
	return []any{&rec.Kind, &rec.Name, &rec.AuxField}
}

func Test_ArgsAndReceivers(t *testing.T) {
	t.Parallel()

	var rec TestRec
	si, err := dbs.NewStructInfo(rec)
	require.NoError(t, err)

	tests := []struct {
		name string
		fn   func(info *dbs.StructInfo, src *TestRec) ([]any, error)
		need []any
	}{
		{name: "InsertOneArgs", fn: adapters.InsertOneArgs[TestRec], need: testRecNonPKRefs(&rec)},
		{name: "InsertOneReceivers", fn: adapters.InsertOneReceivers[TestRec], need: testRecAllRefs(&rec)},
		{name: "SelectOneArgs", fn: adapters.SelectOneArgs[TestRec], need: testRecPKRefs(&rec)},
		{name: "SelectOneReceivers", fn: adapters.SelectOneReceivers[TestRec], need: testRecAllRefs(&rec)},
		{name: "UpdateOneArgs",
			fn:   adapters.UpdateOneArgs[TestRec],
			need: append(testRecNonPKRefs(&rec), testRecPKRefs(&rec)...)},
		{name: "UpdateOneReceivers", fn: adapters.UpdateOneReceivers[TestRec], need: testRecAllRefs(&rec)},
		{name: "DeleteOneArgs", fn: adapters.DeleteOneArgs[TestRec], need: testRecPKRefs(&rec)},
		{name: "DeleteOneReceivers", fn: adapters.DeleteOneReceivers[TestRec], need: testRecAllRefs(&rec)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			refs, errFn := tt.fn(si, &rec)
			require.NoError(t, errFn)
			assert.Equal(t, tt.need, refs)
		})
	}
}
