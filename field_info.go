package dbs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
	"github.com/mirrorru/dot"
)

const (
	tagKey           = "dbs"    // Имя тега для библиотеки
	nameTagKey       = "name"   // Ключ имени
	autoTagKey       = "auto"   // Поле автоматически генерируется в БД
	inlineTagKey     = "inline" // Поле структуры вставляются в родителя
	primaryKeyTagKey = "pk"     // Поле входит в первичный колюч
	refTagKey        = "ref"    // Поле является ссылкой на другую таблицу
	nullTagKey       = "null"   // Поле может быть null, актуально для  ссылок на другую таблицу
)

// FieldInfo - Сведения проецирования поля структуры на поле БД
type FieldInfo struct {
	publicFldConfig
	Type  reflect.Type
	index []int // Составной индекс поля в структуре
	Type  reflect.Type
}

type fieldReference struct {
	StructInfo *StructInfo
	FieldName  string
}
type jointFieldConfig struct {
	publicFldConfig
	privateFldConfig
}

type privateFldConfig struct {
	isReference bool // Ссылается на другую таблицу
	isInline    bool // Надо ли представлять поле как единое целое или как набор полей
}

type publicFldConfig struct {
	RefData    *fieldReference // Данные по ссылке на другие структуры
	Name       string          // Имя поля в БД
	IsAutogen  bool            // Значение поля генерируется самой БД
	IsPK       bool            // Входит в первичный ключ
	IsNullable bool            // Может ли быть NULL
}

func makeFieldConfig(field reflect.StructField) jointFieldConfig {
	var result jointFieldConfig
	if tag := field.Tag.Get(tagKey); tag != "" {
		split := strings.Split(tag, ";")
		for _, s := range split {
			subSplit := strings.Split(s, ":")
			switch subSplit[0] {
			case nameTagKey:
				// Ключ имени
				if len(subSplit) > 1 {
					result.Name = subSplit[1]
				}
			case autoTagKey:
				// Поле автоматически генерируется в БД
				result.IsAutogen = true
			case inlineTagKey:
				// Поля структуры вставляются в родителя
				result.isInline = true
			case primaryKeyTagKey:
				// Поле входит в первичный колюч
				result.IsPK = true
			case refTagKey:
				result.isReference = true
			case nullTagKey:
				result.IsNullable = true
			}
		}
	}
	if result.Name == "" {
		result.Name = dot.ToSnakeCase(field.Name)
	}
	return result
}

type FieldInfoList []FieldInfo

func (fi *FieldInfo) applyIndex(index []int) {
	newIndex := make([]int, 0, len(index)+len(fi.index))
	newIndex = append(newIndex, index...)
	newIndex = append(newIndex, fi.index...)
	fi.index = newIndex
}

func (fi *FieldInfo) applyPrefix(prefix string) {
	fi.Name = prefix + "_" + fi.Name
}

func (fil FieldInfoList) Filter(filterFunc func(fi FieldInfo) (ok bool)) FieldInfoList {
	result := make(FieldInfoList, 0, len(fil))
	for idx := range fil {
		if filterFunc(fil[idx]) {
			result = append(result, fil[idx])
		}
	}
	return result
}

// Refs - получить ссылки на поля структуры из src исходя из определений полей в списке
func (fil FieldInfoList) Refs(refSource any) (result []any, err error) {
	result = make([]any, 0, len(fil)+1) // +1 для добавления оконных функций, если потребуется

	rv := reflect.ValueOf(refSource)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return nil, errStructBasedTypeNeeded
	}

	refDefs := peekStructInfo(rv.Type())
	for idx := range fil {
		refField, ok := refDefs.name2field[fil[idx].Name]
		if !ok {
			return nil, fmt.Errorf("reference field not found [%s]", fil[idx].Name)
		}
		fld := rv.FieldByIndex(refField.index)
		if fld.CanAddr() {
			if fld.Kind() == reflect.Slice {
				result = append(result, pq.Array(fld.Addr().Interface()))
			} else {
				result = append(result, fld.Addr().Interface())
			}
		} else {
			return nil, fmt.Errorf("can't get address for field [%s]", fil[idx].Name)
		}
	}
	return result, nil
}
