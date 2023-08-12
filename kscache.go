package kscache

import (
	"cache/singleflight"
	"fmt"
	"log"
	"sync"
	pb "cache/kscachepb"
)

//If the data does not exist in the cache,
//then call Getter to retrieve the data.
type Getter interface {
	Get(key string) ([]byte, error)
}


type GetterFunc func(key string) ([]byte, error)


func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 当我们需要修改结构体的变量内容的时候，方法传入的结构体变量参数需要使用指针,也就是结构体的地址。 
// 需要修改map中的架构体的变量的时候也需要使用结构体地址作为map的value。 
// 如果仅仅是读取结构体变量，可以不使用指针，直接传递引用即可。

type Group struct {
	name string
	getter Getter
	mainCache cache
	peers PeerPicker
	gg *singleflight.Group

}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {

	req := &pb.Request{
		Group: g.name,
		Key: key,
	}

	resp := &pb.Response{}
	err := peer.Get(req, resp)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: resp.Value}, nil
}
var (
	mu sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {

		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	k := &Group {
		name: name,
		getter: getter,
		mainCache: cache{cacheBytes: cacheBytes},
		gg : &singleflight.Group{},
	}
	groups[name] = k
	return k
}

func GetGroup(name string) *Group {
	mu.RLock()
	k := groups[name]
	mu.RUnlock()
	return k
}


func (g* Group) Get(key string) (ByteView, error) {

	if key == "" {
		return ByteView{}, fmt.Errorf("key is empty")
	}

	if v,ok := g.mainCache.get(key); ok {
		log.Println("[KSCACHE] found")
		return v, nil
	}

	return g.load(key)

}
func (g *Group) load(key string) (value ByteView, err error) {
	view, e := g.gg.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)

	})

	if e == nil {
		return view.(ByteView), e
	}

	return 
}	




func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b : cloneBytes(bytes),}
	g.mainCache.add(key, value)
	return value, nil

}

