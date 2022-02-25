package plugin

import (
	"errors"
	"storage/oceanstor/client"
	"strconv"
	"strings"
	"utils"
	"utils/log"
	"utils/pwd"
)

const (
	DORADO_V6_POOL_USAGE_TYPE     = "0"
)

type OceanstorPlugin struct {
	basePlugin

	cli     *client.Client
	product string
}

func (p *OceanstorPlugin) init(config map[string]interface{}, keepLogin bool) error {
	configUrls, exist := config["urls"].([]interface{})
	if !exist || len(configUrls) <= 0 {
		return errors.New("urls must be provided")
	}

	var urls []string
	for _, i := range configUrls {
		urls = append(urls, i.(string))
	}

	user, exist := config["user"].(string)
	if !exist {
		return errors.New("user must be provided")
	}

	password, exist := config["password"].(string)
	if !exist {
		return errors.New("password must be provided")
	}

	keyText, exist := config["keyText"].(string)
	if !exist {
		return errors.New("keyText must be provided")
	}

	decrypted, err := pwd.Decrypt(password, keyText)
	if err != nil {
		return err
	}

	vstoreName, _ := config["vstoreName"].(string)
	parallelNum, _ := config["parallelNum"].(string)

	cli := client.NewClient(urls, user, decrypted, vstoreName, parallelNum)
	err = cli.Login()
	if err != nil {
		return err
	}

	system, err := cli.GetSystem()
	if err != nil {
		log.Errorf("Get system info error: %v", err)
		return err
	}

	product, err := utils.GetProductVersion(system)
	if err != nil {
		log.Errorf("Get product version error: %v", err)
		return err
	}

	if !keepLogin {
		cli.Logout()
	}

	p.cli = cli
	p.product = product
	return nil
}

func (p *OceanstorPlugin) UpdateBackendCapabilities() (map[string]interface{}, error) {
	features, err := p.cli.GetLicenseFeature()
	if err != nil {
		log.Errorf("Get license feature error: %v", err)
		return nil, err
	}

	log.Debugf("Get license feature: %v", features)

	supportThin := utils.IsSupportFeature(features, "SmartThin")
	supportThick := p.product != "Dorado" && p.product != "DoradoV6"
	supportQoS := utils.IsSupportFeature(features, "SmartQoS")
	supportMetro := utils.IsSupportFeature(features, "HyperMetro")
	supportReplication := utils.IsSupportFeature(features, "HyperReplication")
	supportApplicationType := p.product == "DoradoV6"

	capabilities := map[string]interface{}{
		"SupportThin":        		supportThin,
		"SupportThick":       		supportThick,
		"SupportQoS":         		supportQoS,
		"SupportMetro":       		supportMetro,
		"SupportReplication": 		supportReplication,
		"SupportApplicationType":	supportApplicationType,
	}

	return capabilities, nil
}

func (p *OceanstorPlugin) getParams(name string, parameters map[string]interface{}) map[string]interface{} {
	params := map[string]interface{}{
		"name":        name,
		"description": "Created from Kubernetes CSI",
		"capacity":    utils.RoundUpSize(parameters["size"].(int64), 512),
	}

	paramKeys := []string{
		"storagepool",
		"allocType",
		"qos",
		"authClient",
		"cloneFrom",
		"cloneSpeed",
		"metroDomain",
		"remoteStoragePool",
		"sourceSnapshotName",
		"sourceVolumeName",
		"snapshotParentId",
		"applicationType",
	}

	for _, key := range paramKeys {
		if v, exist := parameters[key]; exist && v != "" {
			params[strings.ToLower(key)] = v
		}
	}

	if v, exist := parameters["hyperMetro"].(string); exist && v != "" {
		params["hypermetro"] = utils.StrToBool(v)
	}

	// Add new bool parameter here
	for _, i := range []string{
		"replication",
	} {
		if v, exist := parameters[i].(string); exist && v != "" {
			params[i] = utils.StrToBool(v)
		}
	}

	// Add new string parameter here
	for _, i := range []string{
		"replicationSyncPeriod",
		"vStorePairID",
	} {
		if v, exist := parameters[i].(string); exist && v != "" {
			params[i] = v
		}
	}

	return params
}

func (p *OceanstorPlugin) updatePoolCapabilities(poolNames []string, usageType string) (map[string]interface{}, error) {
	pools, err := p.cli.GetAllPools()
	if err != nil {
		log.Errorf("Get all pools error: %v", err)
		return nil, err
	}

	log.Debugf("Get pools: %v", pools)

	var validPools []map[string]interface{}
	for _, name := range poolNames {
		if pool, exist := pools[name].(map[string]interface{}); exist {
			poolType, exist := pool["NEWUSAGETYPE"].(string)
			if (pool["USAGETYPE"] == usageType || pool["USAGETYPE"] == DORADO_V6_POOL_USAGE_TYPE) || (
				exist && poolType == DORADO_V6_POOL_USAGE_TYPE) {
				validPools = append(validPools, pool)
			} else {
				log.Warningf("Pool %s is not for %s", name, usageType)
			}
		} else {
			log.Warningf("Pool %s does not exist", name)
		}
	}

	capabilities := p.analyzePoolsCapacity(validPools)
	return capabilities, nil
}

func (p *OceanstorPlugin) analyzePoolsCapacity(pools []map[string]interface{}) map[string]interface{} {
	capabilities := make(map[string]interface{})

	for _, pool := range pools {
		name := pool["NAME"].(string)
		freeCapacity, _ := strconv.ParseInt(pool["USERFREECAPACITY"].(string), 10, 64)

		capabilities[name] = map[string]interface{}{
			"FreeCapacity": freeCapacity * 512,
		}
	}

	return capabilities
}

func (p *OceanstorPlugin) duplicateClient() (*client.Client, error) {
	err := p.cli.Login()
	if err != nil {
		return nil, err
	}

	return p.cli, nil
}
