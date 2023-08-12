package kscache


type ByteView struct {
	b []byte
}


func (bv ByteView) Len() int {
	return len(bv.b)
}

func (bv ByteView) CopyOfB() []byte {
	return cloneBytes(bv.b)
}

func (bv ByteView) String() string {
	return string(bv.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}