package eth

import (
	"strconv"
	"strings"
)

func DecimalToHex(number int64) string {
	return "0x" + strconv.FormatInt(number, 16)
}

func hexaNumberToString(hexaString string) string {
	// replace 0x or 0X with empty String
	numberStr := strings.Replace(hexaString, "0x", "", -1)
	numberStr = strings.Replace(numberStr, "0X", "", -1)
	return numberStr
}

func HexToInt(hex string) (int64, error) {
	return strconv.ParseInt(hexaNumberToString(hex), 16, 64)
}
