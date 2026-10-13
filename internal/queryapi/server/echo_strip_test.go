package server

import "reflect"

// dropEcho zeroes every Input and Ctx field reachable from the pointer: the
// api never writes a validation error's input or ctx (FastAPI's echo of the
// submitted value, which can be a secret), so a golden that still holds them
// is compared without them.
func dropEcho(ptr any) {
	dropEchoValue(reflect.ValueOf(ptr))
}

func dropEchoValue(value reflect.Value) {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			dropEchoValue(value.Elem())
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.Name == "Input" || field.Name == "Ctx" {
				if value.Field(index).CanSet() {
					value.Field(index).SetZero()
				}
				continue
			}
			if field.IsExported() {
				dropEchoValue(value.Field(index))
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			dropEchoValue(value.Index(index))
		}
	}
}
