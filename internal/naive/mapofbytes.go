package naive

import (
	util "github.com/Veckatimest/uniqipgo/internal/util"
)

type IpBytes [4]uint8

func NewMapOfBytes() map[IpBytes]bool {
	return make(map[IpBytes]bool)
}

func AddBytesIp(target map[IpBytes]bool, ip string) (uint32, error) {
	ipBytes, err := util.ParseToOctets(ip)
	if err != nil {
		return 0, err
	}

	if target[ipBytes] {
		return 0, nil
	}

	target[ipBytes] = true
	return 1, nil
}
