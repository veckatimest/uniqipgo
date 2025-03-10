package util

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func ParseToOctets(ip string) ([4]uint8, error) {
	strOctets := strings.Split(ip, ".")

	if len(strOctets) != 4 {
		return [4]uint8{}, fmt.Errorf("Invalid IP %s", ip)
	}

	result := [4]uint8{}
	for i := 0; i < 4; i++ {
		number, err := strconv.Atoi(strOctets[i])
		if err != nil {
			return [4]uint8{}, fmt.Errorf("Invalid octet %s", strOctets[i])
		}
		result[i] = uint8(number)
	}

	return result, nil
}

func ParseRest(ip string) ([3]uint8, error) {
	strOctets := strings.Split(ip, ".")

	if len(strOctets) != 3 {
		return [3]uint8{}, fmt.Errorf("Invalid IP %s", ip)
	}

	result := [3]uint8{}
	for i := 0; i < 3; i++ {
		number, err := strconv.Atoi(strOctets[i])
		if err != nil {
			return [3]uint8{}, fmt.Errorf("Invalid octet %s", strOctets[i])
		}
		result[i] = uint8(number)
	}

	return result, nil
}

type PartialParseResult struct {
	FirstByte uint8
	Rest      string
}

func PartialParse(ip string) (PartialParseResult, error) {
	splitted := strings.SplitN(ip, ".", 2)
	first := splitted[0]
	rest := splitted[1]

	number, err := strconv.Atoi(first)
	if err != nil {
		return PartialParseResult{}, fmt.Errorf("Failed to parse number from %s", first)
	}

	return PartialParseResult{uint8(number), rest}, nil
}

func ParseToUint(ip string) (uint32, error) {
	octets, err := ParseToOctets(ip)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(octets[:]), nil
}
