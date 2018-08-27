package statephoenix

import (
	"testing"
	"database/sql"

	_ "github.com/apache/calcite-avatica-go"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

// TestVDBEnv provides a couch db backed versioned db for testing
type TestVDBEnv struct {
	t          testing.TB
	DBProvider statedb.VersionedDBProvider
}

// NewTestVDBEnv instantiates and new couch db backed TestVDB
func NewTestVDBEnv(t testing.TB) *TestVDBEnv {
	t.Logf("Creating new TestVDBEnv")

	dbProvider, err := NewVersionedDBProvider()
	if err!=nil {
		t.Errorf("NewDBProvider faill")
	}
	testVDBEnv := &TestVDBEnv{t, dbProvider}
	// No cleanup for new test environment.  Need to cleanup per test for each DB used in the test.
	return testVDBEnv
}

// Cleanup drops the test phoenix table and closes the db provider
func (env *TestVDBEnv) Cleanup(dbName string) {
	env.t.Logf("Cleaningup TestVDBEnv")
	env.DBProvider.Close()
	CleanupDB(dbName)
}

// CleanupDB drops the test phoenix table
func CleanupDB(dbName string) {
	//create a new connection
	phoenixDef := GetPhoenixDefinition()
	phoenixInstance, _ := sql.Open(phoenixDef.DriverName, phoenixDef.URL)
	dropString := "DROP TABLE IF EXISTS " + dbName

	phoenixInstance.Exec(dropString)
}

