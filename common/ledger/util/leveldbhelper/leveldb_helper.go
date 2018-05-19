/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leveldbhelper

import (
	"sync"
	"strings"
	"os"
	"context"
	"time"
	"reflect"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var logger = flogging.MustGetLogger("leveldbhelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

// Conf configuration for `DB`
type Conf struct {
	DBPath string
}

// DB - a wrapper on an actual store
type DB struct {
	conf    *Conf
	db      *leveldb.DB
	dbState dbState
	mux     sync.Mutex

	//readOpts        *opt.ReadOptions
	//writeOptsNoSync *opt.WriteOptions
	//writeOptsSync   *opt.WriteOptions

	table string
	host  string
	hbaseAC gohbase.AdminClient
	hbaseC  gohbase.Client
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	//readOpts := &opt.ReadOptions{}
	//writeOptsNoSync := &opt.WriteOptions{}
	//writeOptsSync := &opt.WriteOptions{}
	//writeOptsSync.Sync = true

	dbPath := conf.DBPath
	path := strings.Split(dbPath, "/")
	ledgerhost, _ := os.Hostname()
	table := strings.Replace(path[len(path)-2], "-", "_", -1) + "_" + path[len(path)-1] + ledgerhost
	host := "192.168.16.13:4181"

	hbaseAC := gohbase.NewAdminClient(host)
	hbaseC := gohbase.NewClient(host)

	return &DB{
		conf:            conf,
		dbState:         closed,
		//readOpts:        readOpts,
		//writeOptsNoSync: writeOptsNoSync,
		//writeOptsSync:   writeOptsSync
		table:			table,
		host:			host,
		hbaseAC:		hbaseAC,
		hbaseC:			hbaseC}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == opened {
		return
	}

	key := "whatever"
	headers := map[string][]string{"cf": nil}
	get, err := hrpc.NewGetStr(context.Background(), dbInst.table, key, hrpc.Families(headers))
	_, err = dbInst.hbaseC.Get(get)

	if err == gohbase.TableNotFound {
		for {
			//err := CreateTable(dbInst.hbase_ac, dbInst.table, []string{"colum_f1", "colum_f2"})
			err := CreateTable(dbInst.hbaseAC, dbInst.table, []string{"cf"})
			if err != nil && (strings.Contains(err.Error(), "org.apache.hadoop.hbase.PleaseHoldException") || strings.Contains(err.Error(), "org.apache.hadoop.hbase.ipc.ServerNotRunningYetException")) {
				time.Sleep(time.Second)
				continue
			} else if err != nil {
				logger.Errorf("Error while creating new table", err)
			} else {
				break
			}
		}
	}


	//dbOpts := &opt.Options{}
	//dbPath := dbInst.conf.DBPath
	//var err error
	//var dirEmpty bool
	//if dirEmpty, err = util.CreateDirIfMissing(dbPath); err != nil {
	//	panic(fmt.Sprintf("Error while trying to create dir if missing: %s", err))
	//}
	//dbOpts.ErrorIfMissing = !dirEmpty
	//if dbInst.db, err = leveldb.OpenFile(dbPath, dbOpts); err != nil {
	//	panic(fmt.Sprintf("Error while trying to open DB: %s", err))
	//}
	dbInst.dbState = opened
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == closed {
		return
	}

	dbInst.hbaseAC.(gohbase.Client).Close()
	dbInst.hbaseC.Close()

	//if err := dbInst.db.Close(); err != nil {
	//	logger.Errorf("Error while closing DB: %s", err)
	//}
	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	// get value from hbase
	headers := map[string][]string{"cf": nil}
	get, err_hb := hrpc.NewGetStr(context.Background(), dbInst.table, string(key), hrpc.Families(headers))
	if err_hb != nil {
		logger.Errorf("Error while trying to construct hbase get string %s", err_hb)
	}
	rsp_hb, err_hb := dbInst.hbaseC.Get(get)
	if err_hb == nil && len(rsp_hb.Cells) == 0{
		return []byte(nil), err_hb
		//logger.Errorf("Error while trying to hbase get %s ", err_hb)
	}
	value := rsp_hb.Cells[0].Value

	//value, err := dbInst.db.Get(key, dbInst.readOpts)
	//if err == leveldb.ErrNotFound {
	//	value = nil
	//	err = nil
	//}
	//if err != nil {
	//	logger.Errorf("Error while trying to retrieve key [%#v]: %s", key, err)
	//	return nil, err
	//}
	return value, nil
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	// put into hbase
	if value == nil {
		value = []byte("")
	}
	//values := map[string]map[string][]byte{"cf": map[string][]byte{"value_field_1": value}}
	values := map[string]map[string][]byte{"cf": {"val":value}}
	putRequest, err := hrpc.NewPutStr(context.Background(), dbInst.table, string(key), values)
	if err != nil{
		logger.Errorf("Error while construct put string", err)
		return err
	}
	_, err = dbInst.hbaseC.Put(putRequest)
	if err != nil{
		logger.Errorf("Error while putting value in hbase", err)
		return err
	}
	//

	//wo := dbInst.writeOptsNoSync
	//if sync {
	//	wo = dbInst.writeOptsSync
	//}
	//err := dbInst.db.Put(key, value, wo)
	//if err != nil {
	//	logger.Errorf("Error while trying to write key [%#v]", key)
	//	return err
	//}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	//delete one row from hbase, set value to ""
	//dbInst.Put(key, []byte(""), sync)
	delRequest, err := hrpc.NewDelStr(context.Background(), dbInst.table, string(key), nil)
	if err != nil {
		return err
	}
	_, err = dbInst.hbaseC.Delete(delRequest)
	if err != nil {
		return err
	}else {
		return nil
	}

	//wo := dbInst.writeOptsNoSync
	//if sync {
	//	wo = dbInst.writeOptsSync
	//}
	//err := dbInst.db.Delete(key, wo)
	//if err != nil {
	//	logger.Errorf("Error while trying to delete key [%#v]", key)
	//	return err
	//}
	return nil
}

// GetIterator returns an iterator over key-value store. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) iterator.Iterator {
	//return dbInst.db.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)

	return dbInst.db.NewHbaseIterator(dbInst.hbaseC, dbInst.table, startKey, endKey)
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	// writeBatch in hbase
	for _, v := range batch.Key_Value{
		if !reflect.DeepEqual(v.Value, []byte("del")) {
			err := dbInst.Put(v.Key, v.Value, sync)
			if err != nil {
				return err
			}
		}else {
			dbInst.Delete(v.Key, sync)
		}
	}

	//wo := dbInst.writeOptsNoSync
	//if sync {
	//	wo = dbInst.writeOptsSync
	//}
	//if err := dbInst.db.Write(batch, wo); err != nil {
	//	return err
	//}
	return nil
}

// CreateTable creates the given table with the given families
func CreateTable(client gohbase.AdminClient, table string, cFamilies []string) error {
	// If the table exists, delete it
	//DeleteTable(client, table)
	// Don't check the error, since one will be returned if the table doesn't
	// exist

	cf := make(map[string]map[string]string, len(cFamilies))
	for _, f := range cFamilies {
		cf[f] = nil
	}
	ct := hrpc.NewCreateTable(context.Background(), []byte(table), cf)
	if err := client.CreateTable(ct); err != nil {
		return err
	}

	return nil
}

// DeleteTable finds the HBase shell via the HBASE_HOME environment variable,
// and disables and drops the given table
func (dbInst *DB) DeleteTable() error {
	dit := hrpc.NewDisableTable(context.Background(), []byte(dbInst.table))
	err := dbInst.hbaseAC.DisableTable(dit)
	if err != nil {
		if !strings.Contains(err.Error(), "TableNotEnabledException") {
			return err
		}
	}

	det := hrpc.NewDeleteTable(context.Background(), []byte(dbInst.table))
	err = dbInst.hbaseAC.DeleteTable(det)
	if err != nil {
		return err
	}
	return nil
}