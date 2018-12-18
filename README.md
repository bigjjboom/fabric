# 说明文档
在截至[最后一次提交](https://github.com/bigjjboom/fabric/commit/f46c6df53217254152f632580b77b64ae771c16a)的提交中，进行的改动主要有两处。
**1.将原有本地文件存储改为hdfs存储**
[此处位置](https://github.com/bigjjboom/fabric/tree/release-1.1/common/ledger/blkstorage/fsblkstorage)相对路径为$GOAPTH/fabric/common/ledger/blkstorage/fsblkstorage
这里的改动主要是使用hdfs来代替本地文件读写操作，使得原本保存在本地磁盘的区块文件保存在hadoop集群之上。
具体改动在[这里](https://github.com/bigjjboom/fabric/commit/b5ba78e4fb12ef70c1e923cbb130e338c9b4e211)
[依赖的包](https://github.com/bigjjboom/fabric/tree/release-1.1/vendor/github.com/colinmarc/hdfs)已加入vendor之中
**2.为statedb做一个可以条件查询的基于大数据平台的实现**
[此处位置](https://github.com/bigjjboom/fabric/tree/release-1.1/core/ledger/kvledger/txmgmt/statedb/statephoenix)相对路径为$GOAPTH/fabric/core/ledger/kvledger/txmgmt/statedb/statephoenix
这里的主要内容是根据[versiondb接口定义](https://github.com/bigjjboom/fabric/blob/release-1.1/core/ledger/kvledger/txmgmt/statedb/statedb.go)，参照statecouchdb和stateleveldb实现相关功能。
在[最后一次提交中](https://github.com/bigjjboom/fabric/commit/f46c6df53217254152f632580b77b64ae771c16a)除条件查询之外均已实现前面接口定义的其他功能。
唯一的无法解决的问题在func (vdb *VersionedDB)ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error)实现上，由于查询结果将使用RPC回传给链码，所以查询出来的rows结果集无法通过序列化和反序列化被使用，同时也因为条件查询的结果具有不确定性，无法将rows中列数不定的结果使用变量取出组成VersionedValue数组进行返回 。
[依赖的包](https://github.com/bigjjboom/fabric/tree/release-1.1/vendor/github.com/Boostport/avatica)也已加入vendor之中
