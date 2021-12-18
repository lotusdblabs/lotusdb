package lotusdb

type Txn struct {
	writes []interface{}
}

func (db *LotusDB) NewTxn(update bool) *Txn {
	return nil
}

func (db *LotusDB) NewOptimisticTxn() {

}

func (db *LotusDB) Txn(fn func(txn *Txn) error) error {
	//txn := db.NewTxn(true)
	return nil
}

func (db *LotusDB) OptimisticTxn() {

}
