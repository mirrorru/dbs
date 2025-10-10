package dbs

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/mirrorru/dot"
)

var (
	structInfoMap = dot.SyncStore[reflect.Type, *StructInfo]{}

	errStructBasedTypeNeeded = errors.New("value is not a struct-based")
)

// TableNamer - интерфейс для получения имени таблицы в БД
type TableNamer interface {
	TableName() string
}

type StructInfo struct {
	onceInit      sync.Once
	structType    reflect.Type
	tableName     string
	allFields     FieldInfoList
	pkFields      FieldInfoList
	autoFields    FieldInfoList
	nonPkFields   FieldInfoList
	nonAutoFields FieldInfoList
	name2field    map[string]FieldInfo
}

func NewStructInfo(src any) (*StructInfo, error) {
	srcType := reflect.TypeOf(src)
	for srcType.Kind() == reflect.Slice || srcType.Kind() == reflect.Array || srcType.Kind() == reflect.Ptr {
		srcType = srcType.Elem()
	}

	if srcType.Kind() != reflect.Struct {
		return nil, errStructBasedTypeNeeded
	}

	return peekStructInfo(srcType), nil
}

func (s *StructInfo) init(srcType reflect.Type) {
	s.onceInit.Do(func() {
		s.structType = srcType
		structValue := reflect.New(s.structType)
		if tableNamer, ok := structValue.Interface().(TableNamer); ok {
			s.tableName = tableNamer.TableName()
		} else {
			s.tableName = s.structType.Name()
		}

		s.allFields = getFieldInfo(s.structType)
		s.pkFields = make(FieldInfoList, 0, 2)
		s.autoFields = make(FieldInfoList, 0, 3)
		s.nonPkFields = make(FieldInfoList, 0, len(s.allFields))
		s.nonAutoFields = make(FieldInfoList, 0, len(s.allFields))

		s.name2field = make(map[string]FieldInfo, len(s.allFields))

		for _, field := range s.allFields {
			if _, ok := s.name2field[field.Name]; ok {
				panic(fmt.Errorf("duplicate field name [%s]", field.Name))
			}
			if field.IsPK {
				s.pkFields = append(s.pkFields, field)
			} else {
				s.nonPkFields = append(s.nonPkFields, field)
			}
			if field.IsAutogen {
				s.autoFields = append(s.autoFields, field)
			} else {
				s.nonAutoFields = append(s.nonAutoFields, field)
			}
			s.name2field[field.Name] = field
		}
	})
}

func (s *StructInfo) Type() reflect.Type {
	return s.structType
}

func (s *StructInfo) AllFields() FieldInfoList {
	return s.allFields
}

func (s *StructInfo) PKFields() FieldInfoList {
	return s.pkFields
}

func (s *StructInfo) NonPKFields() FieldInfoList {
	return s.nonPkFields
}

func (s *StructInfo) AutoFields() FieldInfoList {
	return s.autoFields
}

func (s *StructInfo) NonAutoFields() FieldInfoList {
	return s.nonAutoFields
}

func (s *StructInfo) TableName() string {
	return s.tableName
}

func peekStructInfo(srcType reflect.Type) *StructInfo {
	info := structInfoMap.GetOrPut(srcType, func() *StructInfo { return &StructInfo{} })
	info.init(srcType)

	return info
}

//nolint:gocognit
func getFieldInfo(t reflect.Type) FieldInfoList {
	resultList := make(FieldInfoList, 0, t.NumField())
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue // Пропускаем неэкспортируемые поля
		}

		if field.Anonymous {
			info := peekStructInfo(field.Type)
			for _, fld := range info.allFields {
				fld.applyIndex(field.Index)
				resultList = append(resultList, fld)
			}

			continue
		}

		fieldCfg := makeFieldConfig(field)

		if fieldCfg.isInline && field.Type.Kind() == reflect.Struct {
			info := peekStructInfo(field.Type)
			for _, fld := range info.allFields {
				fld.applyIndex(field.Index)
				fld.applyPrefix(fieldCfg.Name)
				resultList = append(resultList, fld)
			}

			continue
		}

		if fieldCfg.isReference && (field.Type.Kind() == reflect.Struct || field.Type.Kind() == reflect.Ptr) {
			info := peekStructInfo(field.Type)
			for _, fld := range info.pkFields {
				fld.RefData = &fieldReference{
					StructInfo: info,
					FieldName:  fld.Name,
				}
				fld.IsPK, fld.IsAutogen = false, false
				fld.IsNullable = fld.IsNullable || field.Type.Kind() == reflect.Ptr
				fld.applyIndex(field.Index)
				fld.applyPrefix(fieldCfg.Name)
				resultList = append(resultList, fld)
			}

			continue
		}

		fi := makeFieldInfo(field, fieldCfg.publicFldConfig)
		resultList = append(resultList, fi)
	}

	return resultList
}

func makeFieldInfo(field reflect.StructField, cfg publicFldConfig) FieldInfo {
	result := FieldInfo{
		publicFldConfig: cfg,
		Type:            field.Type,
		index:           field.Index,
	}
	if result.Name == "" {
		result.Name = dot.ToSnakeCase(field.Name)
	}

	return result
}
