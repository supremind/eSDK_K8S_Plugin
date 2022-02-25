package attacher

import (
	"connector"
	"storage/oceanstor/client"
	"utils"
	"utils/log"
)

type OceanStorAttacher struct {
	Attacher
}

const (
	MULTIPATHTYPE_DEFAULT = "0"
)

func newOceanStorAttacher(
	cli *client.Client,
	protocol,
	invoker string,
	portals []string,
	alua map[string]interface{}) AttacherPlugin {
	return &OceanStorAttacher{
		Attacher: Attacher{
			cli:      cli,
			protocol: protocol,
			invoker:  invoker,
			portals:  portals,
			alua:     alua,
		},
	}
}

func (p *OceanStorAttacher) needUpdateInitiatorAlua(initiator map[string]interface{}, hostAlua map[string]interface{}) bool {
	multiPathType, ok := hostAlua["MULTIPATHTYPE"]
	if !ok {
		return false
	}

	if multiPathType != initiator["MULTIPATHTYPE"] {
		return true
	} else if initiator["MULTIPATHTYPE"] == MULTIPATHTYPE_DEFAULT {
		return false
	}

	failoverMode, ok := hostAlua["FAILOVERMODE"]
	if ok && failoverMode != initiator["FAILOVERMODE"] {
		return true
	}

	specialModeType, ok := hostAlua["SPECIALMODETYPE"]
	if ok && specialModeType != initiator["SPECIALMODETYPE"] {
		return true
	}

	pathType, ok := hostAlua["PATHTYPE"]
	if ok && pathType != initiator["PATHTYPE"] {
		return true
	}

	return false
}

func (p *OceanStorAttacher) attachISCSI(hostID, hostName string) error {
	iscsiInitiator, err := p.Attacher.attachISCSI(hostID)
	if err != nil {
		return err
	}

	hostAlua := utils.GetAlua(p.alua, hostName)
	if hostAlua != nil && p.needUpdateInitiatorAlua(iscsiInitiator, hostAlua) {
		err = p.cli.UpdateIscsiInitiator(iscsiInitiator["ID"].(string), hostAlua)
	}

	return err
}

func (p *OceanStorAttacher) attachFC(hostID, hostName string) error {
	fcInitiators, err := p.Attacher.attachFC(hostID)
	if err != nil {
		return err
	}

	hostAlua := utils.GetAlua(p.alua, hostName)
	if hostAlua != nil {
		for _, i := range fcInitiators {
			if !p.needUpdateInitiatorAlua(i, hostAlua) {
				continue
			}

			err := p.cli.UpdateFCInitiator(i["ID"].(string), hostAlua)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *OceanStorAttacher) attachRoCE(hostID string) error {
	_, err := p.Attacher.attachRoCE(hostID)
	return err
}

func (p *OceanStorAttacher) ControllerAttach(lunName string, parameters map[string]interface{}) (
	map[string]interface{}, error) {
	host, err := p.getHost(parameters, true)
	if err != nil {
		log.Errorf("Get host ID error: %v", err)
		return nil, err
	}

	hostID := host["ID"].(string)
	hostName := host["NAME"].(string)

	if p.protocol == "iscsi" {
		err = p.attachISCSI(hostID, hostName)
	} else if p.protocol == "fc" || p.protocol == "fc-nvme" {
		err = p.attachFC(hostID, hostName)
	} else if p.protocol == "roce" {
		err = p.attachRoCE(hostID)
	}

	if err != nil {
		log.Errorf("Attach %s connection error: %v", p.protocol, err)
		return nil, err
	}

	wwn, hostLunId, err := p.doMapping(hostID, lunName)
	if err != nil {
		log.Errorf("Mapping LUN %s to host %s error: %v", lunName, hostID, err)
		return nil, err
	}

	return p.getMappingProperties(wwn, hostLunId)
}

// NodeStage to do storage mapping and get the connector
func (p *OceanStorAttacher) NodeStage(lunName string, parameters map[string]interface{}) (
	*connector.ConnectInfo, error) {
	return connectVolume(p, lunName, p.protocol, parameters)
}
