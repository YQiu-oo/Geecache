package kscache

import (
	consistenthash "cache/consistentHash"	
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	pb "cache/kscachepb"
	"github.com/golang/protobuf/proto"

)

const (
	default_path = "/kscache/"
	defaultReplicas = 50

)




type httpGetter struct {
	baseURL string
}


func (h *httpGetter) Get(in *pb.Request, out * pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	res,err := http.Get(u)

	if err != nil {
		return err
	}

	defer res.Body.Close()


	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	if err = proto.Unmarshal(bytes, out) ; err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil

}


type HttpPool struct {
	self string
	basePath string
	peers *consistenthash.Map
	httpGetters map[string]*httpGetter
	mu sync.Mutex 
}

// Set updates the pool's list of peers.
func (p *HttpPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}
var _ PeerGetter = (*httpGetter)(nil)

// PickPeer picks a peer according to key
func (p *HttpPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}


func New(self string) *HttpPool {
	return &HttpPool{
		self: self,
		basePath: default_path,
	}
}

// Log info with server name
func (p *HttpPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HttpPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if  !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HttpPool serving unexpectedd path : " + r.URL.Path)
	}

	p.Log("%s %s", r.Method, r.URL.Path)
	
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)

	if (len(parts) != 2) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	body, err := proto.Marshal(&pb.Response{Value: view.CopyOfB()})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)

}
