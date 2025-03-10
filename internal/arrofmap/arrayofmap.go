package arrofmap

import (
	"sync"

	"github.com/Veckatimest/uniqipgo/internal/util"
)

type IpBytes [4]byte

type SafeMap struct {
	sync.Mutex
	storage map[IpBytes]bool
}

// MapStorage allows us to use concurrency for maps
type MapStorage struct {
	children [256]*SafeMap
}

func (ms *MapStorage) AddIp(ip string) (uint32, error) {
	ipBytes, err := util.ParseToOctets(ip)

	if err != nil {
		return 0, err
	}

	idxStore := ms.children[ipBytes[3]]
	idxStore.Lock()
	defer idxStore.Unlock()
	if idxStore.storage[ipBytes] {
		return 0, nil
	}

	idxStore.storage[ipBytes] = true
	return 1, nil
}

func NewArrayOfMap() *MapStorage {
	storage := &MapStorage{}

	for i := 0; i < 256; i++ {
		storage.children[i] = &SafeMap{
			storage: make(map[IpBytes]bool),
		}
	}

	return storage
}
