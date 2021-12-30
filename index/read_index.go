package index

import (
	"errors"

	"github.com/flowercorp/lotusdb/index/boltdb"
	"github.com/flowercorp/lotusdb/index/domain"
)

func NewIndexComponent(rc domain.IndexComponentConf) (r domain.IndexComponent, err error) {
	switch rc.GetType() {
	case boltdb.IndexComponentTyp:
		bboltdbConfig, ok := rc.(*boltdb.BboltdbConfig)
		if !ok {
			return nil, errors.New("config type error")
		}
		return boltdb.NewBboltdb(bboltdbConfig)
	default:
		return nil, errors.New("unknow type")
	}
}
