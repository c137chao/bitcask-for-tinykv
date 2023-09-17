package utils

import "strconv"

func FloatToBytes(fval float64) []byte {
	return []byte(strconv.FormatFloat(fval, 'f', -1, 64))
}

func FloatFromBytes(fbytes []byte) float64 {
	fval, _ := strconv.ParseFloat(string(fbytes), 64)
	return fval
}
