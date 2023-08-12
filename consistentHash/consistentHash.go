package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv")

type Hash func(data []byte) uint32

type Map struct {
	hash       Hash
	hashMap    map[int]string
	replicates int
	keys       []int
}
//Constucctor is used to initialize attributes
func New(replicates int, fn Hash) *Map {
	m := &Map{
		replicates: replicates,
		hashMap: make(map[int]string),
		hash: fn,
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) Add(key ...string) {
	for _, key := range key {
		for i := 0; i < m.replicates; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx %len(m.keys)]]
}