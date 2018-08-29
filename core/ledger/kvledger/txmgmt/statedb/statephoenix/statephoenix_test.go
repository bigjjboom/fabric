package statephoenix

import (
	"os"
	"testing"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/commontests"
)

func TestMain(m *testing.M) {
	// Read the core.yaml file for default config.
	ledgertestutil.SetupCoreYAMLConfig()
	viper.Set("peer.fileSystemPath", "/tmp/fabric/ledgertests/kvledger/txmgmt/statedb/statephoenix")

	// Switch to Phoenix
	viper.Set("ledger.state.stateDatabase", "Phoenix")

	// both vagrant and CI have couchdb configured at host "couchdb"
	viper.Set("ledger.state.phoenixConfig.driverName", "avatica")
	viper.Set("ledger.state.phoenixConfig.phoenixDBAddress", "http://localhost:8765")
	viper.Set("ledger.state.phoenixConfig.phoenixTablePattern", " (cpK VARCHAR PRIMARY KEY,  ns VARCHAR, pk VARCHAR, pval VARCHAR, blknum BIGINT, txnum BIGINT)")

	os.Exit(m.Run())
}

func TestBasicRW(t *testing.T) {
	//env := NewTestVDBEnv(t)
	//env.Cleanup("testbasicrw")
	//defer env.Cleanup("testbasicrw")
	//commontests.TestBasicRW(t, env.DBProvider)
}
func TestMultiDBBasicRW(t *testing.T) {
	//env := NewTestVDBEnv(t)
	//env.Cleanup("testmultidbbasicrw")
	//env.Cleanup("testmultidbbasicrw2")
	//defer env.Cleanup("testmultidbbasicrw")
	//defer env.Cleanup("testmultidbbasicrw2")
	//commontests.TestMultiDBBasicRW(t, env.DBProvider)
}
func TestDeletes(t *testing.T) {
	//env := NewTestVDBEnv(t)
	//env.Cleanup("testdeletes")
	//defer env.Cleanup("testdeletes")
	//commontests.TestDeletes(t, env.DBProvider)
}
func TestIterator(t *testing.T) {
	//env := NewTestVDBEnv(t)
	//env.Cleanup("testiterator")
	//defer env.Cleanup("testiterator")
	//commontests.TestIterator(t, env.DBProvider)
}
func TestEncodeDecodeValueAndVersion(t *testing.T) {
	//testValueAndVersionEncoding(t, []byte("value1"), version.NewHeight(1, 2))
	//testValueAndVersionEncoding(t, []byte{}, version.NewHeight(50, 50))
}
func testValueAndVersionEncoding(t *testing.T, value []byte, version *version.Height) {
	encodedValue := statedb.EncodeValue(value, version)
	val, ver := statedb.DecodeValue(encodedValue)
	testutil.AssertEquals(t, val, value)
	testutil.AssertEquals(t, ver, version)
}
func TestQuery(t *testing.T) {
	env := NewTestVDBEnv(t)
	//env.Cleanup("testquery")
	//defer env.Cleanup("testquery")
	commontests.TestQuery(t, env.DBProvider)
}
func TestGetStateMultipleKeys(t *testing.T) {}
func TestGetVersion(t *testing.T) {}
func TestSmallBatchSize(t *testing.T) {}
func TestBatchRetry(t *testing.T) {}

