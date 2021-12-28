package index

import (
	"errors"

	"github.com/flowercorp/lotusdb/index/boltdb"
	"github.com/flowercorp/lotusdb/index/domain"
)

func NewReadIndexComponent(rc domain.ReadIndexComponentConf) (r domain.ReadIndexComponent, err error) {
	switch rc.GetType() {
	case boltdb.ReadIndexComponentTyp:
		bboltdbConfig, ok := rc.(*boltdb.BboltdbConfig)
		if !ok {
			return nil, errors.New("config type error")
		}
		return boltdb.NewBboltdb(bboltdbConfig)
	default:
		return nil, errors.New("unknow type")
	}
}
