package statephoenix

import (
	"sync"
	"database/sql"

	_ "github.com/apache/calcite-avatica-go"
	"github.com/spf13/viper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("statephoenix")

type VersionedDBProvider struct {
	dbProvider *sql.DB
	tablePattern string
	dbName map[string]*VersionedDB

	mux sync.Mutex
}

func NewVersionedDBProvider() (*VersionedDBProvider, error)  {
	phoenixDef := GetPhoenixDefinition()
	phoenixConn, err := sql.Open(phoenixDef.DriverName, phoenixDef.URL)
	if err!= nil {
		return nil, err
	}
	return &VersionedDBProvider{phoenixConn, phoenixDef.TablePattern, make(map[string]*VersionedDB), sync.Mutex{}}, nil
}

func (provider *VersionedDBProvider)GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()

	vdb := provider.dbName[dbName]
	if vdb == nil{
		vdb, err := newVersionedDB(provider.dbProvider, dbName, provider.tablePattern)
		if err != nil{
			return nil, err
		}
		provider.dbName[dbName] = vdb
	}
	//vdb still is nil?
	vdb = provider.dbName[dbName]
	return vdb, nil
}

func (provider *VersionedDBProvider)Close() {
	//close Phoenix connection
	//provider.dbProvider.Close()
}

type VersionedDB struct {
	phoenixConn *sql.DB
	tableName string

	mux sync.RWMutex
}

func newVersionedDB(dbProvider *sql.DB, dbName string, tablePattern string) (*VersionedDB, error) {
	err := createPhoenixTable(dbProvider, dbName, tablePattern)
	if err!=nil {
		logger.Error(err)
		return nil, err
	}

	return &VersionedDB{dbProvider, dbName, sync.RWMutex{}}, nil
}

func (vdb *VersionedDB)GetState(namespace string, key string) (*statedb.VersionedValue, error)  {
	logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	compositeKey := constructCompositeKey(namespace, key)
	sqlString := `SELECT * FROM ` + vdb.tableName + ` WHERE cpK = '` + compositeKey + `'`
	row, err := queryRows(vdb.phoenixConn, sqlString)
	defer row.Close()
	if err != nil {
		return nil, err
	}
	var(
		compositeKeyInTable string
		namespaceInTable 	string
		keyInTable		  	string
		//should replace value to values
		valueInTable	  	string
		blockNumInTable		uint64
		txNumInTable		uint64
	)
	for row.Next() {
		err = row.Scan(&compositeKeyInTable, &namespaceInTable, &keyInTable, &valueInTable, &blockNumInTable, &txNumInTable)
		if err != nil {
			return nil, err
		}
	}
	if compositeKeyInTable=="" && namespaceInTable=="" && keyInTable=="" && valueInTable=="" && blockNumInTable==0 && txNumInTable==0{
		return nil, nil
	}
	return &statedb.VersionedValue{Value:[]byte(valueInTable), Version:version.NewHeight(blockNumInTable, txNumInTable)}, nil
}

func (vdb *VersionedDB)GetVersion(namespace string, key string) (*version.Height, error)  {
	versionedValue, err := vdb.GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	if versionedValue ==nil {
		return nil,nil
	}
	return versionedValue.Version, nil
}

func (vdb *VersionedDB)GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error)  {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

func (vdb *VersionedDB)GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error)  {
	sqlString := `SELECT * FROM ` + vdb.tableName + ` WHERE ns = '` + namespace + `'`
	if startKey != "" && endKey == "" {
		sqlString = sqlString + ` AND ` + `pk >= '` + startKey + `'`
	}
	if startKey == "" && endKey != "" {
		sqlString = sqlString + ` AND ` + `pk < '` + endKey + `'`
	}
	if startKey != "" && endKey !="" {
		sqlString = sqlString + ` AND ` + `pk >= '` + startKey + `' AND pk < '` + endKey + `'`
	}
	//logger.Errorf("line 138 out [%s]", sqlString)
	rows, err := queryRows(vdb.phoenixConn, sqlString)
	if err != nil {
		return nil, err
	}
	return newkvScanner(namespace, rows), nil
}

func (vdb *VersionedDB)ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error)  {
	logger.Debugf("ExecuteQuery(). ns=%s, query=%s", namespace, query)
	result, err := queryRows(vdb.phoenixConn, query)
	if err != nil{
		return nil, err
	}

	return newqueryScanner(namespace, result), nil
}

func (vdb *VersionedDB)ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error  {
	namespace := batch.GetUpdatedNamespaces()
	for _, ns := range namespace  {
		updates := batch.GetUpdates(ns)
		sqlDelete := `DELETE FROM `+ vdb.tableName +` WHERE cpK = '`
		sqlUpsert := `UPSERT INTO ` + vdb.tableName + ` VALUES(?, ?, ?, ?, ?, ?)`

		for k, vv := range updates {
			var(
				compositeKeyInTable string	=	constructCompositeKey(ns, k)
				namespaceInTable 	string	=	ns
				keyInTable		  	string	=	k
				//should replace value to values
				valueInTable	  	string	=	string(vv.Value)
				blockNumInTable		uint64	=	vv.Version.BlockNum
				txNumInTable		uint64	=	vv.Version.TxNum
			)
			logger.Debugf("Channel [%s]: Applying key(string)=[%s]", vdb.tableName, compositeKeyInTable)

			if vv.Value == nil {
				sqlDeleteString := sqlDelete + compositeKeyInTable + `'`
				_, err := vdb.phoenixConn.Exec(sqlDeleteString)
				if err != nil {
					return err
				}
			} else {
				_, err := vdb.phoenixConn.Exec(sqlUpsert, compositeKeyInTable, namespaceInTable, keyInTable, valueInTable, blockNumInTable, txNumInTable)
				if err != nil {
					return err
				}
			}
		}
	}
	sqlUpsertSavePoint := `UPSERT INTO ` + vdb.tableName + ` VALUES(?, ?, ?, ?, ?, ?)`
	var(
				compositeKeyInTable string	=	"savepoint"
				namespaceInTable 	string	=	""
				keyInTable		  	string	=	""
				//should replace value to values
				valueInTable	  	string	=	""
				blockNumInTable		uint64	=	height.BlockNum
				txNumInTable		uint64	=	height.TxNum
	)
	vdb.phoenixConn.Exec(sqlUpsertSavePoint, compositeKeyInTable, namespaceInTable, keyInTable, valueInTable, blockNumInTable, txNumInTable)

	return nil
}

func (vdb *VersionedDB)GetLatestSavePoint() (*version.Height, error)  {
	savePomit := "savepoint"
	sqlString := `SELECT * FROM ` + vdb.tableName + ` WHERE cpK = '` + savePomit + `'`
	row, err := queryRows(vdb.phoenixConn, sqlString)
	defer row.Close()
	if err != nil {
		return nil, err
	}

	var(
		compositeKeyInTable string
		namespaceInTable 	string
		keyInTable		  	string
		//should replace value to values
		valueInTable	  	string
		blockNumInTable		uint64
		txNumInTable		uint64
	)
	for row.Next() {
		err = row.Scan(&compositeKeyInTable, &namespaceInTable, &keyInTable, &valueInTable, &blockNumInTable, &txNumInTable)
		if err != nil {
			return nil, err
		}
	}
	if blockNumInTable == 0 && txNumInTable == 0 {
		return nil, nil
	}
	return &version.Height{blockNumInTable, txNumInTable}, nil
}

func (vdb *VersionedDB)ValidateKeyValue(key string, value []byte) error  {
	return nil
}

func (vdb *VersionedDB)BytesKeySuppoted() bool  {
	return false
}

func (vdb *VersionedDB)Open() error  {
	return nil
}

func (vdb *VersionedDB)Close()  {
	//Do nothing
}

func createPhoenixTable(dbProvider *sql.DB, dbName string, tablePattern string) error {
	//create table if not exists?

	sqlString := `CREATE TABLE IF NOT EXISTS ` + dbName + tablePattern

	_, err := dbProvider.Exec(sqlString)
	return err
}

func queryRows(dbProvider *sql.DB, sqlString string) (*sql.Rows, error) {
	rows, err := dbProvider.Query(sqlString)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type PhoenixDef struct {
	DriverName string
	URL		   string
	TablePattern string
}

func GetPhoenixDefinition() *PhoenixDef {
	driverName := viper.GetString("ledger.state.phoenixConfig.driverName")
	phoenixDBAddress := viper.GetString("ledger.state.phoenixConfig.phoenixDBAddress")
	// tablePattern must like this " (cpK VARCHAR PRIMARY KEY,  ns VARCHAR, pk VARCHAR, pval VARCHAR, blknum BIGINT, txnum BIGINT)"
	// all types can be find in https://github.com/apache/calcite-avatica-go/blob/master/driver_phoenix_test.go
	tablePattern := viper.GetString("ledger.state.phoenixConfig.phoenixTablePattern")

	return &PhoenixDef{driverName, phoenixDBAddress, tablePattern}
}

type kvScanner struct {
	namespace string
	rows      *sql.Rows
}

func newkvScanner(namespace string, rows *sql.Rows) *kvScanner {
	return &kvScanner{namespace, rows}
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error)  {
	var(
		compositeKeyInTable string
		namespaceInTable 	string
		keyInTable		  	string
		//should replace value to values
		valueInTable	  	string
		blockNumInTable		uint64
		txNumInTable		uint64
	)
	if !scanner.rows.Next() {
		return nil, nil
	}

	err := scanner.rows.Scan(&compositeKeyInTable, &namespaceInTable, &keyInTable, &valueInTable, &blockNumInTable, &txNumInTable)
	if err != nil {
		return nil, err
	}
	//logger.Errorf("line 309 out [%s] [%s] [%s] [%s] [%d] [%d]", compositeKeyInTable, namespaceInTable, keyInTable, valueInTable, blockNumInTable, txNumInTable)
	return &statedb.VersionedKV{
		CompositeKey:statedb.CompositeKey{namespaceInTable, keyInTable},
		VersionedValue:statedb.VersionedValue{[]byte(valueInTable), &version.Height{blockNumInTable, txNumInTable}}}, nil
}

func (scanner *kvScanner) Close(){
	scanner.rows.Close()
}

type queryScaaner struct {
	Namespace string
	Result	  *sql.Rows
}

func newqueryScanner(namespace string, rows *sql.Rows) *queryScaaner {
	return &queryScaaner{namespace, rows}
}
func (scanner *queryScaaner)Next() (statedb.QueryResult, error) {
	return nil,nil
}
func (scanner *queryScaaner)Close() {
	scanner.Result.Close()
}
func constructCompositeKey(ns string, key string) string {
	return ns + "GAP" + key
}