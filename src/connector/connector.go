package connector

import (
	"fmt"

	"github.com/Huawei/eSDK_K8S_Plugin/src/utils/log"
)

const (
	FCDriver               = "fibreChannel"
	FCNVMeDriver           = "FC-NVMe"
	ISCSIDriver            = "iSCSI"
	RoCEDriver             = "RoCE"
	LocalDriver            = "Local"
	NFSDriver              = "NFS"
	MountFSType            = "fs"
	MountBlockType         = "block"
	flushMultiPathInternal = 20
	intNumFour             = 4
	deviceWWidLength       = 4
	halfMiDataLength       = 524288
)

var connectors = map[string]Connector{}

type Connector interface {
	ConnectVolume(map[string]interface{}) (string, error)
	DisConnectVolume(string) error
}

// DisConnectInfo defines the fields of disconnect volume
type DisConnectInfo struct {
	Conn   Connector
	TgtLun string
}

// ConnectInfo defines the fields of connect volume
type ConnectInfo struct {
	Conn        Connector
	MappingInfo map[string]interface{}
}

func GetConnector(cType string) Connector {
	if cnt, exist := connectors[cType]; exist {
		return cnt
	}

	log.Errorf("%s is not registered to connector", cType)
	return nil
}

func RegisterConnector(cType string, cnt Connector) error {
	if _, exist := connectors[cType]; exist {
		return fmt.Errorf("connector %s already exists", cType)
	}

	connectors[cType] = cnt
	return nil
}
