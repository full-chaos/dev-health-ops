package pybody

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// PydanticInt is pydantic's lax int: a bool or int as is; a finite float with no
// fraction; a str pydantic-core parses as an int.
func PydanticInt(value pyjson.Value) (*big.Int, string, string) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return big.NewInt(1), "", ""
		}
		return big.NewInt(0), "", ""
	case pyjson.Int:
		return typed.Int, "", ""
	case pyjson.Float:
		f := float64(typed)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, "finite_number", "Input should be a finite number"
		}
		if f != math.Trunc(f) {
			return nil, "int_from_float", "Input should be a valid integer, got a number with a fractional part"
		}
		if !(f > -9223372036854775808 && f < 9223372036854775808) {
			return nil, "int_parsing_size", "Unable to parse input string as an integer, exceeded maximum size"
		}
		number, _ := new(big.Float).SetFloat64(f).Int(nil)
		return number, "", ""
	case string:
		number, failure := ParsePydanticInt(typed)
		if failure != nil {
			return nil, failure.Type, failure.Msg
		}
		return number, "", ""
	}
	return nil, "int_type", "Input should be a valid integer"
}

// PydanticDatetime is pydantic's lax datetime of a decoded JSON value
// (python mode): pytime.ParseDatetime with a JSON number passed as the
// number it is.
func PydanticDatetime(value pyjson.Value) (pytime.DateTime, *pytime.ValidationError) {
	switch typed := value.(type) {
	case pyjson.Int:
		return pytime.ParseDatetime(typed.Int)
	case pyjson.Float:
		return pytime.ParseDatetime(float64(typed))
	}
	return pytime.ParseDatetime(value)
}

// PydanticFloat is pydantic's lax float: a bool, int or float as a float; a str
// pydantic-core parses as a float.
func PydanticFloat(value pyjson.Value) (float64, string, string) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, "", ""
		}
		return 0, "", ""
	case pyjson.Int:
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		// pydantic-core refuses an int whose nearest float is infinite
		// (float_type, no parse detail), the halfway point above MaxFloat64
		// included: it rounds to even, to infinity.
		if math.IsInf(f, 0) {
			return 0, "float_type", "Input should be a valid number"
		}
		return f, "", ""
	case pyjson.Float:
		return float64(typed), "", ""
	case string:
		f, ok := ParsePydanticFloat(typed)
		if !ok {
			return 0, "float_parsing", "Input should be a valid number, unable to parse string as a number"
		}
		return f, "", ""
	}
	return 0, "float_type", "Input should be a valid number"
}

// ParsePydanticFloat is pydantic-core's str-to-float: surrounding
// whitespace stripped, ASCII only, "_" only between two digits (then
// dropped), and the decimal grammar with an exponent, or inf, infinity or
// nan in any case, with a sign. An overflow is an infinity.
func ParsePydanticFloat(text string) (float64, bool) {
	trimmed := strings.TrimFunc(text, unicode.IsSpace)
	if trimmed == "" {
		return 0, false
	}
	var cleaned strings.Builder
	for index := 0; index < len(trimmed); index++ {
		c := trimmed[index]
		if c >= 0x80 || c == 'x' || c == 'X' || c == 'p' || c == 'P' {
			return 0, false
		}
		if c == '_' {
			if index == 0 || index == len(trimmed)-1 || !isDigit(trimmed[index-1]) || !isDigit(trimmed[index+1]) {
				return 0, false
			}
			continue
		}
		cleaned.WriteByte(c)
	}
	f, err := strconv.ParseFloat(cleaned.String(), 64)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
			return f, true
		}
		return 0, false
	}
	return f, true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
