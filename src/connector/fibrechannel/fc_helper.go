package fibrechannel

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Huawei/eSDK_K8S_Plugin/src/connector"
	"github.com/Huawei/eSDK_K8S_Plugin/src/utils"
	"github.com/Huawei/eSDK_K8S_Plugin/src/utils/log"
)

type target struct {
	tgtWWN     string
	tgtHostLun string
}

type rawDevice struct {
	platform string
	pciNum   string
	wwn      string
	lun      string
}

type deviceInfo struct {
	tries          int
	hostDevice     string
	realDeviceName string
}

type connectorInfo struct {
	tgtLunWWN          string
	tgtWWNs            []string
	tgtHostLUNs        []string
	tgtTargets         []target
	volumeUseMultiPath bool
}

const (
	deviceScanAttemptsDefault int = 3
	intNumTwo                 int = 2
)

func scanHost() {
	output, err := utils.ExecShellCmd("for host in $(ls /sys/class/fc_host/); " +
		"do echo \"- - -\" > /sys/class/scsi_host/${host}/scan; done")
	if err != nil {
		log.Warningf("rescan fc_host error: %s", output)
	}
}

func parseFCInfo(connectionProperties map[string]interface{}) (*connectorInfo, error) {
	tgtLunWWN, LunWWNExist := connectionProperties["tgtLunWWN"].(string)
	if !LunWWNExist {
		msg := "there is no target Lun WWN in the connection info"
		log.Errorln(msg)
		return nil, errors.New(msg)
	}

	tgtWWNs, WWNsExist := connectionProperties["tgtWWNs"].([]string)
	if !WWNsExist {
		msg := "there are no target WWNs in the connection info"
		log.Errorln(msg)
		return nil, errors.New(msg)
	}

	tgtHostLUNs, hostLunIdExist := connectionProperties["tgtHostLUNs"].([]string)
	if !hostLunIdExist {
		msg := "there are no target hostLun in the connection info"
		log.Errorln(msg)
		return nil, errors.New(msg)
	}

	volumeUseMultiPath, useMultiPathExist := connectionProperties["volumeUseMultiPath"].(bool)
	if !useMultiPathExist {
		msg := "there are no multiPath switch in the connection info"
		log.Errorln(msg)
		return nil, errors.New(msg)
	}

	if tgtWWNs == nil || len(tgtWWNs) != len(tgtHostLUNs) {
		msg := "the numbers of tgtWWNs and tgtHostLUNs are not equal"
		log.Errorln(msg)
		return nil, errors.New(msg)
	}

	var con connectorInfo
	con.tgtLunWWN = tgtLunWWN
	con.tgtWWNs = tgtWWNs
	con.tgtHostLUNs = tgtHostLUNs
	con.volumeUseMultiPath = volumeUseMultiPath
	return &con, nil
}

func constructFCInfo(conn *connectorInfo) {
	for index := range conn.tgtWWNs {
		conn.tgtTargets = append(conn.tgtTargets, target{conn.tgtWWNs[index],
			conn.tgtHostLUNs[index]})
	}
}

func tryConnectVolume(connMap map[string]interface{}) (string, error) {
	conn, err := parseFCInfo(connMap)
	if err != nil {
		return "", err
	}

	constructFCInfo(conn)
	hbas, err := getFcHBAsInfo()
	if err != nil {
		return "", err
	}

	hostDevice := getPossibleVolumePath(conn.tgtTargets, hbas)
	devInfo, err := waitDeviceDiscovery(hbas, hostDevice, conn.tgtTargets, conn.volumeUseMultiPath)
	if err != nil {
		return "", err
	}

	if devInfo.realDeviceName == "" {
		log.Warningln("No FibreChannel volume device found")
		return "", errors.New("NoFibreChannelVolumeDeviceFound")
	}

	log.Infof("Found Fibre Channel volume %v (after %d rescans.)", devInfo, devInfo.tries+1)
	if !conn.volumeUseMultiPath {
		device := fmt.Sprintf("/dev/%s", devInfo.realDeviceName)
		err := connector.VerifySingleDevice(device, conn.tgtLunWWN,
			"NoFibreChannelVolumeDeviceFound", false, tryDisConnectVolume)
		if err != nil {
			return "", err
		}
		return device, nil
	}

	// realPath: dm-<id>
	log.Infof("Start to find the dm multiapth of device %s", devInfo.realDeviceName)
	mPath := connector.FindAvailableMultiPath([]string{devInfo.realDeviceName})
	if mPath != "" {
		dev, err := connector.VerifyMultiPathDevice(mPath, conn.tgtLunWWN,
			"NoFibreChannelVolumeDeviceFound", false, tryDisConnectVolume)
		if err != nil {
			return "", err
		}
		return dev, nil
	}

	log.Warningf("can not find device for lun", conn.tgtLunWWN)
	return "", errors.New("NoFibreChannelVolumeDeviceFound")
}

func getHostInfo(host, portAttr string) (string, error) {
	output, err := utils.ExecShellCmd("cat /sys/class/fc_host/%s/%s", host, portAttr)
	if err != nil {
		log.Errorf("Get host %s FC initiator Attr %s output: %s", host, portAttr, output)
		return "", err
	}

	return output, nil
}

func getHostAttrName(host, portAttr string) (string, error) {
	nodeName, err := getHostInfo(host, portAttr)
	if err != nil {
		return "", err
	}

	lines := strings.Split(nodeName, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "0x") {
			continue
		}
		attrWwn := line[2:]
		return attrWwn, nil
	}

	msg := fmt.Sprintf("Can not find the %s of host %s", portAttr, host)
	log.Errorln(msg)
	return "", errors.New(msg)
}

func isPortOnline(host string) (bool, error) {
	output, err := utils.ExecShellCmd("cat /sys/class/fc_host/%s/port_state", host)
	if err != nil {
		return false, err
	}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		return line == "Online", nil
	}

	return false, errors.New("check port state error")
}

func getClassDevicePath(host string) (string, error) {
	hostPath := fmt.Sprintf("/sys/class/fc_host/%s", host)
	classDevicePath, err := filepath.EvalSymlinks(hostPath)
	if err != nil || classDevicePath == "" {
		msg := fmt.Sprintf("Get host %s class device path failed.", host)
		log.Errorln(msg)
		return "", errors.New(msg)
	}

	return classDevicePath, nil
}

func getAllFcHosts() ([]string, error) {
	output, err := utils.ExecShellCmd("ls /sys/class/fc_host/")
	if err != nil {
		return nil, err
	}

	var hosts []string
	hostLines := strings.Fields(output)
	for _, h := range hostLines {
		host := strings.TrimSpace(h)
		hosts = append(hosts, host)
	}

	return hosts, nil
}

func getAvailableFcHBAsInfo() ([]map[string]string, error) {
	allFcHosts, err := getAllFcHosts()
	if err != nil {
		return nil, err
	}
	if allFcHosts == nil {
		return nil, errors.New("there is no fc host")
	}

	var hbas []map[string]string
	for _, h := range allFcHosts {
		hbaInfo, err := getFcHbaInfo(h)
		if err != nil {
			log.Warningf("Get Fc HBA info error %v", err)
			continue
		}
		hbas = append(hbas, hbaInfo)
	}
	log.Infof("Get available hbas are %v", hbas)
	return hbas, nil
}

func getFcHbaInfo(host string) (map[string]string, error) {
	online, err := isPortOnline(host)
	if err != nil || !online {
		return nil, errors.New("the port state is not available")
	}

	portName, err := getHostAttrName(host, "port_name")
	if err != nil {
		return nil, errors.New("the port name is not available")
	}

	nodeName, err := getHostAttrName(host, "node_name")
	if err != nil {
		return nil, errors.New("the node name is not available")
	}

	classDevicePath, err := getClassDevicePath(host)
	if err != nil {
		return nil, errors.New("the device path is not available")
	}

	hba := map[string]string{
		"port_name":   portName,
		"node_name":   nodeName,
		"host_device": host,
		"device_path": classDevicePath,
	}
	return hba, nil
}

func getFcHBAsInfo() ([]map[string]string, error) {
	if !supportFC() {
		return nil, errors.New("no Fibre Channel support detected on system")
	}

	hbas, err := getAvailableFcHBAsInfo()
	if err != nil || hbas == nil {
		return nil, errors.New("there is no available port")
	}

	return hbas, nil
}

func supportFC() bool {
	fcHostSysFSPath := "/sys/class/fc_host"
	if exist, _ := utils.PathExist(fcHostSysFSPath); !exist {
		return false
	}

	return true
}

func getPossibleVolumePath(targets []target, hbas []map[string]string) []string {
	possibleDevices := getPossibleDeices(hbas, targets)
	return getHostDevices(possibleDevices)
}

func getPossibleDeices(hbas []map[string]string, targets []target) []rawDevice {
	var rawDevices []rawDevice
	for _, hba := range hbas {
		platform, pciNum := getPciNumber(hba)
		if pciNum != "" {
			for _, target := range targets {
				targetWWN := fmt.Sprintf("0x%s", strings.ToLower(target.tgtWWN))
				rawDev := rawDevice{platform, pciNum, targetWWN, target.tgtHostLun}
				rawDevices = append(rawDevices, rawDev)
			}
		}
	}

	return rawDevices
}

func getPci(devPath []string) (string, string) {
	var platform string
	platformSupport := len(devPath) > 3 && devPath[3] == "platform"
	for index, value := range devPath {
		if platformSupport && strings.HasPrefix(value, "pci") {
			platform = fmt.Sprintf("platform-%s", devPath[index-1])
		}
		if strings.HasPrefix(value, "net") || strings.HasPrefix(value, "host") {
			return platform, devPath[index-1]
		}
	}
	return "", ""
}

func getPciNumber(hba map[string]string) (string, string) {
	if hba == nil {
		return "", ""
	}

	if _, exist := hba["device_path"]; exist {
		devPath := strings.Split(hba["device_path"], "/")
		platform, device := getPci(devPath)
		if device != "" {
			return platform, device
		}
	}

	return "", ""
}

func formatLunId(lunId string) string {
	intLunId, _ := strconv.Atoi(lunId)
	if intLunId < 256 {
		return lunId
	} else {
		return fmt.Sprintf("0x%04x%04x00000000", intLunId&0xffff, intLunId>>16&0xffff)
	}
}

func getHostDevices(possibleDevices []rawDevice) []string {
	var hostDevices []string
	var platform string
	for _, value := range possibleDevices {
		if value.platform != "" {
			platform = value.platform + "-"
		} else {
			platform = ""
		}

		hostDevice := fmt.Sprintf("/dev/disk/by-path/%spci-%s-fc-%s-lun-%s", platform, value.pciNum, value.wwn, formatLunId(value.lun))
		hostDevices = append(hostDevices, hostDevice)
	}
	return hostDevices
}

func checkValidDevice(dev string) bool {
	_, err := connector.ReadDevice(dev)
	if err != nil {
		return false
	}

	return true
}

func waitDeviceDiscovery(hbas []map[string]string, hostDevices []string, targets []target, volumeUseMultiPath bool) (
	deviceInfo, error) {
	var info deviceInfo
	err := utils.WaitUntil(func() (bool, error) {
		rescanHosts(hbas, targets, volumeUseMultiPath)
		for _, dev := range hostDevices {
			if exist, _ := utils.PathExist(dev); exist && checkValidDevice(dev) {
				info.hostDevice = dev
				if realPath, err := os.Readlink(dev); err == nil {
					info.realDeviceName = filepath.Base(realPath)
				}
				return true, nil
			}
		}

		if info.tries >= deviceScanAttemptsDefault {
			log.Errorln("Fibre Channel volume device not found.")
			return false, errors.New("NoFibreChannelVolumeDeviceFound")
		}

		info.tries += 1
		return false, nil
	}, time.Second*60, time.Second*2)
	return info, err
}

func getHBAChannelSCSITargetLun(hba map[string]string, targets []target) ([][]string, []string) {
	hostDevice := hba["host_device"]
	if hostDevice != "" && len(hostDevice) > 4 {
		hostDevice = hostDevice[4:]
	}

	path := fmt.Sprintf("/sys/class/fc_transport/target%s:", hostDevice)

	var channelTargetLun [][]string
	var lunNotFound []string
	for _, tar := range targets {
		cmd := fmt.Sprintf("grep -Gil \"%s\" %s*/port_name", tar.tgtWWN, path)
		output, err := utils.ExecShellCmd(cmd)
		if err != nil {
			lunNotFound = append(lunNotFound, tar.tgtHostLun)
			continue
		}

		lines := strings.Split(output, "\n")
		var tempCtl [][]string
		for _, line := range lines {
			if strings.HasPrefix(line, path) {
				ctl := append(strings.Split(strings.Split(line, "/")[4], ":")[1:], tar.tgtHostLun)
				tempCtl = append(tempCtl, ctl)
			}
		}

		channelTargetLun = append(channelTargetLun, tempCtl...)
	}

	return channelTargetLun, lunNotFound
}

func rescanHosts(hbas []map[string]string, targets []target, volumeUseMultiPath bool) {
	var process []interface{}
	var skipped []interface{}
	for _, hba := range hbas {
		ctls, lunWildCards := getHBAChannelSCSITargetLun(hba, targets)
		if ctls != nil {
			process = append(process, []interface{}{hba, ctls})
		} else if process == nil {
			var lunInfo [][]string
			for _, lun := range lunWildCards {
				lunInfo = append(lunInfo, []string{"-", "-", lun})
			}
			skipped = append(skipped, []interface{}{hba, lunInfo})
		}
	}

	if process == nil {
		process = skipped
	}

	for _, p := range process {
		pro, ok := p.([]interface{})
		if !ok {
			log.Errorf("the %v is not interface", p)
			return
		}

		if len(pro) != intNumTwo {
			log.Errorf("the length of %s not equal 2", pro)
			return
		}

		hba := pro[0].(map[string]string)
		if !ok {
			log.Errorf("the %v is not map[string]string", pro[0])
			return
		}

		ctls := pro[1].([][]string)
		if !ok {
			log.Errorf("the %v is not [][]string", pro[1])
			return
		}

		for _, c := range ctls {
			scanFC(c, hba["host_device"])
			if !volumeUseMultiPath {
				break
			}
		}
		if !volumeUseMultiPath {
			break
		}
	}
}

func scanFC(channelTargetLun []string, hostDevice string) {
	scanCommand := fmt.Sprintf("echo \"%s %s %s\" > /sys/class/scsi_host/%s/scan",
		channelTargetLun[0], channelTargetLun[1], channelTargetLun[2], hostDevice)
	output, err := utils.ExecShellCmd(scanCommand)
	if err != nil {
		log.Warningf("rescan FC host error: %s", output)
	}
}

func tryDisConnectVolume(tgtLunWWN string, checkDeviceAvailable bool) error {
	return connector.DisConnectVolume(tgtLunWWN, checkDeviceAvailable, tryToDisConnectVolume)
}

func tryToDisConnectVolume(tgtLunWWN string, checkDeviceAvailable bool) error {
	device, err := connector.GetDevice(nil, tgtLunWWN, checkDeviceAvailable)
	if err != nil {
		log.Warningf("Get device of WWN %s error: %v", tgtLunWWN, err)
		return err
	}

	multiPathName, err := connector.RemoveDevice(device)
	if err != nil {
		log.Errorf("Remove device %s error: %v", device, err)
		return err
	}

	if multiPathName != "" {
		err = connector.FlushDMDevice(device)
		if err != nil {
			return err
		}
	}

	return nil
}
