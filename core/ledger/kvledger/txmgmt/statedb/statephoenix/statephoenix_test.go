package statephoenix

import (
	"os"
	"testing"
	"github.com/spf13/viper"
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
	env := NewTestVDBEnv(t)
	env.Cleanup("testbasicrw_")
	env.Cleanup("testbasicrw_ns")
	env.Cleanup("testbasicrw_ns1")
	env.Cleanup("testbasicrw_ns2")
	defer env.Cleanup("testbasicrw_")
	defer env.Cleanup("testbasicrw_ns")
	defer env.Cleanup("testbasicrw_ns1")
	defer env.Cleanup("testbasicrw_ns2")
	commontests.TestBasicRW(t, env.DBProvider)
}
func TestMultiDBBasicRW(t *testing.T) {}
func TestDeletes(t *testing.T) {}
func TestIterator(t *testing.T) {}
func TestEncodeDecodeValueAndVersion(t *testing.T) {}
func TestQuery(t *testing.T) {}
func TestGetStateMultipleKeys(t *testing.T) {}
func TestGetVersion(t *testing.T) {}
func TestSmallBatchSize(t *testing.T) {}
func TestBatchRetry(t *testing.T) {}

