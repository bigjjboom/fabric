package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

func (c *Client) Truncate(name string, newLength uint64) error {
    req := &hdfs.TruncateRequestProto {
        Src:        proto.String(name),
        NewLength:  proto.Uint64(newLength),
        ClientName: proto.String(c.namenode.ClientName()),
    }
    resp := &hdfs.TruncateResponseProto{}

	err := c.namenode.Execute("truncate", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"truncate", name, err}
	} else if resp.Result == nil {
		return &os.PathError{
			"truncate",
			name,
			errors.New("Unexpected empty response to 'truncate' rpc call"),
		}
	}

	return nil
}