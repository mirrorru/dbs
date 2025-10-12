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

	return peekStructInfo(srcType)
}

var errStructInitFailure = errors.New("struct init failure")

func (s *StructInfo) init(srcType reflect.Type) (err error) {
	s.onceInit.Do(func() {
		s.structType = srcType
		structValue := reflect.New(s.structType)
		if tableNamer, ok := structValue.Interface().(TableNamer); ok {
			s.tableName = tableNamer.TableName()
		} else {
			s.tableName = dot.ToSnakeCase(s.structType.Name())
		}

		if s.allFields, err = getFieldInfo(s.structType); err != nil {
			return
		}
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
	if s.name2field == nil {
		err = errStructInitFailure
	}
	return err
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

func peekStructInfo(srcType reflect.Type) (*StructInfo, error) {
	if srcType.Kind() != reflect.Struct {
		return nil, errStructBasedTypeNeeded
	}

	info := structInfoMap.GetOrPut(srcType, func() *StructInfo { return &StructInfo{} })
	err := info.init(srcType)

	return info, err
}

//nolint:gocognit
func getFieldInfo(t reflect.Type) (resultList FieldInfoList, err error) {
	resultList = make(FieldInfoList, 0, t.NumField())
	for i := range t.NumField() {
		var info *StructInfo
		field := t.Field(i)
		if !field.IsExported() {
			continue // Пропускаем неэкспортируемые поля
		}

		fieldCfg := makeFieldConfig(field)

		switch field.Type.Kind() {
		case reflect.Ptr:
			target := field.Type.Elem()
			if target.Kind() != reflect.Struct {
				break //switch
			}
			if info, err = peekStructInfo(target); err != nil {
				return nil, err
			}
			if len(info.pkFields) != 1 {
				return nil, fmt.Errorf("structure of reference %s must have single PK field", field.Type.Name())
			}
			for _, fld := range info.pkFields {
				if fieldCfg.isReference {
					fld.RefData = &fieldReference{
						StructInfo: info,
						FieldName:  fld.Name,
					}
				}
				fld.IsPK, fld.IsAutogen = fieldCfg.IsPK, fieldCfg.IsAutogen
				fld.IsNullable = true
				fld.applyPrefix(fieldCfg.Name)
				resultList = append(resultList, makeFieldInfo(field, fld.publicFldConfig))
			}
			continue
		case reflect.Struct:
			if !field.Anonymous && !fieldCfg.isInline {
				break
			}
			if info, err = peekStructInfo(field.Type); err != nil {
				return nil, err
			}
			for _, fld := range info.allFields {
				fld.applyIndex(field.Index)
				if fieldCfg.isInline {
					fld.applyPrefix(fieldCfg.Name)
				}
				resultList = append(resultList, fld)
			}
			continue
		}
		resultList = append(resultList, makeFieldInfo(field, fieldCfg.publicFldConfig))
	}

	return resultList, nil
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
