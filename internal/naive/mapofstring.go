package naive

func AddStringIp(target map[string]bool, ip string) uint32 {
	if target[ip] {
		return 0
	}

	target[ip] = true
	return 1
}
