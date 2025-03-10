package naive

import (
	util "github.com/Veckatimest/uniqipgo/internal/util"
)

func NewMapOfUint() map[uint32]bool {
	return make(map[uint32]bool)
}

func AddUintIp(target map[uint32]bool, ip string) (uint32, error) {
	uintIp, err := util.ParseToUint(ip)
	if err != nil {
		return 0, err
	}

	if target[uintIp] {
		return 0, nil
	}

	target[uintIp] = true
	return 1, nil
}
