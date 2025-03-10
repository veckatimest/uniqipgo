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
			return [4]uint8{}, fmt.Errorf("Invalid octet '%s'", strOctets[i])
		}
		result[i] = uint8(number)
	}

	return result, nil
}

func ParseToUint(ip string) (uint32, error) {
	octets, err := ParseToOctets(ip)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(octets[:]), nil
}
