package naive

func AddStringIp(target map[string]bool, ip string) (uint32, error) {
	if target[ip] {
		return 0, nil
	}

	target[ip] = true
	return 1, nil
}
