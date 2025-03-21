package nps_mux

import (
	"sync"
)

type connMap struct {
	cMap sync.Map
	//closeCh chan struct{}
}

func NewConnMap() *connMap {
	return &connMap{
		cMap: sync.Map{},
	}
}

func (s *connMap) Get(id int32) (*conn, bool) {
	v, ok := s.cMap.Load(id)
	if ok && v != nil {
		c, ok1 := v.(*conn)
		if c != nil && ok1 {
			return c, true
		}
	}
	return nil, false
}

func (s *connMap) Set(id int32, v *conn) {
	s.cMap.Store(id, v)
}

func (s *connMap) Close() {
	s.cMap.Range(func(key, value interface{}) bool {
		_ = value.(*conn).Close()
		return true
	})
}

func (s *connMap) Delete(id int32) {
	s.cMap.Delete(id)
}
