/*
 Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.

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

// Package connector provide the common func of scan device
package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Huawei/eSDK_K8S_Plugin/src/utils"
	"github.com/Huawei/eSDK_K8S_Plugin/src/utils/log"
)

func getDeviceLink(tgtLunGUID string) (string, error) {
	output, err := utils.ExecShellCmd("ls -l /dev/disk/by-id/ | grep %s", tgtLunGUID)
	if err != nil {
		if strings.TrimSpace(output) == "" || strings.Contains(output, "No such file or directory") {
			return "", nil
		}

		return "", err
	}
	return output, nil
}

func getDevice(findDeviceMap map[string]string, deviceLink string) string {
	var dev string
	devLines := strings.Split(deviceLink, "\n")
	for _, line := range devLines {
		splits := strings.Split(line, "../../")
		if len(splits) >= 2 {
			name := splits[1]

			if strings.HasPrefix(name, "dm") {
				dev = name
				break
			}

			if _, exist := findDeviceMap[name]; !exist && strings.HasPrefix(name, "nvme") {
				dev = name
				break
			}

			if _, exist := findDeviceMap[name]; !exist && strings.HasPrefix(name, "sd") {
				dev = name
				break
			}
		}
	}
	return dev
}

// GetDevice query device from host. If revert connect volume, no need to check device available
func GetDevice(findDeviceMap map[string]string, tgtLunGUID string, checkDeviceAvailable bool) (string, error) {
	deviceLink, err := getDeviceLink(tgtLunGUID)
	if err != nil {
		return "", err
	}

	dev := getDevice(findDeviceMap, deviceLink)
	if dev == "" {
		return "", errors.New("FindNoDevice")
	}

	devPath := fmt.Sprintf("/dev/%s", dev)
	if exist, _ := utils.PathExist(devPath); !exist {
		return "", nil
	}

	if !checkDeviceAvailable {
		return dev, nil
	}

	// If the device can not read, directly return the device
	readable := isDeviceReadable(devPath)
	if !readable {
		return dev, nil
	}

	if strings.HasPrefix(dev, "dm") {
		devices, err := getDeviceFromDM(dev)
		if err != nil {
			return "", err
		}

		_, err = IsMultiPathAvailable(dev, tgtLunGUID, devices)
		if err != nil {
			return "", err
		}
	} else {
		_, err = IsDeviceAvailable(devPath, tgtLunGUID)
		if err != nil {
			return "", err
		}
	}

	return dev, nil
}

func DeleteDMDev(dm string) error {
	err := FlushDMDevice(dm)
	if err != nil {
		return err
	}

	output, err := utils.ExecShellCmd("ls /sys/block/%s/slaves/", dm)
	devices := strings.Split(output, "\n")
	for _, device := range devices {
		err = DeleteNVMEDev(device)
		if err != nil {
			log.Errorf("Delete nvme error: %v", device)
			return err
		}
	}

	return nil
}

func DeleteNVMEDev(nvme string) error {
	output, err := utils.ExecShellCmd("echo 1 > /sys/block/%s/device/rescan_controller", nvme)
	if err != nil {
		if strings.Contains(output, "No such file or directory") {
			return nil
		}

		log.Errorf("Delete NVME device %s error: %v", nvme, output)
		return err
	}

	return nil
}

func DeleteDevice(tgtLunGuid string) error {
	var findDeviceMap map[string]string

	for i := 0; i < 10; i++ {
		device, err := GetDevice(findDeviceMap, tgtLunGuid, true)
		if err != nil && err.Error() != "FindNoDevice" {
			log.Errorf("Get device of GUID %s error: %v", tgtLunGuid, err)
			return err
		}

		if strings.HasPrefix(device, "dm") {
			err = DeleteDMDev(device)
		} else if match, _ := regexp.MatchString(`nvme[0-9]+n[0-9]+`, device); match {
			err = DeleteNVMEDev(device)
		} else {
			log.Warningf("Device of Guid %s to delete does not exist anymore", tgtLunGuid)
			return nil
		}

		if err != nil {
			log.Errorf("Delete %s error: %v", device, err)
			return err
		}

		time.Sleep(time.Second * 2)
	}

	return fmt.Errorf("delete device of Guid %s timeout", tgtLunGuid)
}

func reScanNVMe(device string) error {
	if match, _ := regexp.MatchString(`nvme[0-9]+n[0-9]+`, device); match {
		output, err := utils.ExecShellCmd("echo 1 > /sys/block/%s/device/rescan_controller", device)
		if err != nil {
			log.Warningf("rescan nvme path error: %s", output)
			return err
		}
	} else if match, _ := regexp.MatchString(`nvme[0-9]+$`, device); match {
		output, err := utils.ExecShellCmd("nvme ns-rescan /dev/%s", device)
		if err != nil {
			log.Warningf("rescan nvme path error: %s", output)
			return err
		}
	}
	return nil
}

func ScanNVMe(connectInfo map[string]interface{}) {
	protocol := connectInfo["protocol"].(string)
	var devices []string
	if protocol == "iscsi" {
		output, err := utils.ExecShellCmd("nvme list-subsys -o json")
		if err != nil {
			log.Errorf("get exist nvme connect port error: %s", err)
			return
		}

		var nvmeConnectInfo map[string]interface{}
		if err = json.Unmarshal([]byte(output), &nvmeConnectInfo); err != nil {
			log.Errorf("Failed to unmarshal input %s", output)
			return
		}

		subSystems := nvmeConnectInfo["Subsystems"].([]interface{})
		var allSubPaths []interface{}
		for _, s := range subSystems {
			subSystem := s.(map[string]interface{})
			if strings.Contains(subSystem["NQN"].(string), connectInfo["targetNqn"].(string)) {
				allSubPaths = subSystem["Paths"].([]interface{})
				break
			}
		}

		for _, p := range allSubPaths {
			path := p.(map[string]interface{})
			devices = append(devices, path["Name"].(string))
		}
	} else {
		output, err := utils.ExecShellCmd("ls /dev | grep nvme")
		if err != nil {
			log.Errorf("get nvme path error: %s", output)
			return
		}

		devices = strings.Split(output, "\n")
	}

	for _, device := range devices {
		// ignore the error when scan nvme device, because will not find the device
		_ = reScanNVMe(device)
	}
}

func getDeviceFromDM(dm string) ([]string, error) {
	devPath := fmt.Sprintf("/sys/block/%s/slaves/*", dm)
	paths, err := filepath.Glob(devPath)
	if err != nil {
		return nil, err
	}

	var devices []string
	for _, path := range paths {
		_, dev := filepath.Split(path)
		devices = append(devices, dev)
	}
	return devices, nil
}

func DeleteSDDev(sd string) error {
	output, err := utils.ExecShellCmd("echo 1 > /sys/block/%s/device/delete", sd)
	if err != nil {
		if strings.Contains(output, "No such file or directory") {
			return nil
		}

		log.Errorf("Delete SD device %s error: %v", sd, output)
		return err
	}
	return nil
}

func FlushDMDevice(dm string) error {
	// command awk can always return success, just check the output
	mPath, _ := utils.ExecShellCmd("ls -l /dev/mapper/ | grep -w %s | awk '{print $9}'", dm)
	if mPath == "" {
		return fmt.Errorf("get DM device %s", dm)
	}

	var err error
	for i := 0; i < 3; i++ {
		_, err = utils.ExecShellCmd("multipath -f %s", mPath)
		if err == nil {
			log.Infof("Flush multipath device %s successful", mPath)
			break
		}
		log.Warningf("Flush multipath device %s error: %v", mPath, err)
		time.Sleep(time.Second * flushMultiPathInternal)
	}

	return err
}

func flushDeviceIO(devPath string) error {
	output, err := utils.ExecShellCmd("blockdev --flushbufs %s", devPath)
	if err != nil {
		if strings.Contains(output, "No such device") {
			return nil
		}

		log.Warningf("Failed to flush IO buffers prior to removing device %s", devPath)
	}

	return nil
}

func removeSCSIDevice(sd string) error {
	devPath := fmt.Sprintf("/dev/%s", sd)
	err := flushDeviceIO(devPath)
	if err != nil {
		log.Errorf("Flush %s error: %v", devPath, err)
		return err
	}

	err = DeleteSDDev(sd)
	if err != nil {
		log.Errorf("Delete %s error: %v", sd, err)
		return err
	}

	waitVolumeRemoval([]string{sd})
	return nil
}

func waitVolumeRemoval(devPaths []string) {
	existPath := devPaths
	for index := 0; index <= 30; index++ {
		var exist []string
		for _, dev := range existPath {
			_, err := os.Stat(dev)
			if err != nil && os.IsNotExist(err) {
				log.Infof("The dev %s has been deleted", dev)
			} else {
				exist = append(exist, dev)
			}
		}

		existPath = exist
		if len(existPath) == 0 {
			return
		}

		if index < 30 {
			time.Sleep(time.Second)
		}
	}

	return
}

func removeSymlinks(devices []string, realPath, link string) error {
	for _, dev := range devices {
		if dev == realPath {
			if err := os.Remove(link); err != nil {
				return fmt.Errorf("failed to unlink: %+v", err)
			}
		}
	}

	return nil
}

func removeSCSISymlinks(devices []string) error {
	links, err := filepath.Glob("/dev/disk/by-id/scsi-*")
	if err != nil {
		return err
	}

	for _, link := range links {
		if _, err := os.Lstat(link); os.IsNotExist(err) {
			return nil
		}

		realPath, err := os.Readlink(link)
		if err != nil {
			return err
		}

		err = removeSymlinks(devices, realPath, link)
		if err != nil {
			return err
		}
	}

	return nil
}

func getSessionIdByDevice(devPath string) (string, error) {
	dev := fmt.Sprintf("/sys/block/%s", devPath)
	realPath, err := os.Readlink(dev)
	if err != nil {
		return "", err
	}

	file := strings.Split(realPath, "/session")
	if len(file) == 0 {
		return "", nil
	}

	return strings.Split(file[1], "/")[0], nil
}

func RemoveDeviceConnection(device string) ([]string, string, error) {
	var multiPathName string
	var devSessionIds []string
	var err error
	if strings.HasPrefix(device, "dm") {
		multiPathName = device
		devices, _ := getDeviceFromDM(multiPathName)
		for _, dev := range devices {
			sessionId, err := getSessionIdByDevice(dev)
			if err != nil {
				return nil, "", err
			}
			devSessionIds = append(devSessionIds, sessionId)
		}

		multiPathName, err = removeMultiPathDevice(device, devices)
	} else if strings.HasPrefix(device, "sd") {
		sessionId, err := getSessionIdByDevice(device)
		if err != nil {
			return nil, "", err
		}
		devSessionIds = append(devSessionIds, sessionId)

		err = removeSCSIDevice(device)
	} else {
		log.Warningf("Device %s to delete does not exist anymore", device)
	}

	if err != nil {
		return nil, "", err
	}

	return devSessionIds, multiPathName, nil
}

func waitForPath(volumePath string) bool {
	for i := 0; i < 3; i++ {
		if exist, _ := utils.PathExist(volumePath); exist {
			return true
		}
		time.Sleep(time.Second * 3)
	}
	return false
}

func FindMultiDevicePath(tgtWWN string) string {
	path := fmt.Sprintf("/dev/disk/by-id/dm-uuid-mpath-%s", tgtWWN)
	if waitForPath(path) {
		return path
	}

	path = fmt.Sprintf("/dev/mapper/%s", tgtWWN)
	if waitForPath(path) {
		return path
	}

	return ""
}

// FindAvailableMultiPath is to get dm-multiapth through sd devices
func FindAvailableMultiPath(foundDevices []string) string {
	mPathMap, mPath := findMultiPathMaps(foundDevices)
	if len(mPathMap) == 1 {
		return mPath
	}

	if len(mPathMap) == 0 {
		log.Infof("Start to clean up the devices %s", foundDevices)
		if err := removeDevices(foundDevices); err != nil {
			log.Errorf("clear devices %v error %v", foundDevices, err)
			return ""
		}
		return ""
	}

	for dmPath, devices := range mPathMap {
		log.Infof("Start to clean up the multipath %s with devices %s", dmPath, devices)
		if _, err := removeMultiPathDevice(dmPath, devices); err != nil {
			log.Errorf("clear multipath %s and devices %v error %v", dmPath, devices, err)
		}
	}

	return ""
}

func findMultiPathMaps(foundDevices []string) (map[string][]string, string) {
	mPathMap := make(map[string][]string)
	var mPath string
	for _, device := range foundDevices {
		dmPath := fmt.Sprintf("/sys/block/%s/holders/dm-*", device)

		paths, err := filepath.Glob(dmPath)
		if err != nil || paths == nil {
			continue
		}

		splitPath := strings.Split(paths[0], "/")
		mPath = splitPath[len(splitPath)-1]
		mPathMap[mPath] = append(mPathMap[mPath], device)
	}

	return mPathMap, mPath
}

func getSCSIWwnByScsiID(hostDevice string) (string, error) {
	cmd := fmt.Sprintf("/lib/udev/scsi_id --page 0x83 --whitelisted %s", hostDevice)
	output, err := utils.ExecShellCmd(cmd)
	if err != nil {
		log.Errorf("Failed to get scsi id of device %s, err is %v", hostDevice, err)
		return "", err
	}

	return strings.TrimSpace(output), nil
}

func getScsiHostWWid(devInfo map[string]string) (string, error) {
	wwIDFile := fmt.Sprintf("/sys/class/scsi_host/host%s/device/session*/target%s:%s:%s/%s:%s:%s:%s/wwid",
		devInfo["host"], devInfo["host"], devInfo["channel"], devInfo["id"], devInfo["host"],
		devInfo["channel"], devInfo["id"], devInfo["lun"])

	output, err := utils.ExecShellCmd("cat %s", wwIDFile)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(output, "\n"), nil
}

func getFCHostWWid(devInfo map[string]string) (string, error) {
	wwIDFile := fmt.Sprintf("/sys/class/fc_host/host%s/device/rport-%s:%s-%s/target%s:%s:%s/%s:%s:%s:%s/wwid",
		devInfo["host"], devInfo["host"], devInfo["channel"], devInfo["id"],
		devInfo["host"], devInfo["channel"], devInfo["id"],
		devInfo["host"], devInfo["channel"], devInfo["id"], devInfo["lun"])

	output, err := utils.ExecShellCmd("cat %s", wwIDFile)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(output, "\n"), nil
}

func getSCSIWwnByWWid(hostDevice string) (string, error) {
	devInfo := getDeviceInfo(strings.Split(hostDevice, "dev/")[1])
	if devInfo == nil {
		return "", errors.New("can not get device info")
	}

	var data string
	var err error
	data, err = getScsiHostWWid(devInfo)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			data, err = getFCHostWWid(devInfo)
		}

		if err != nil {
			msg := fmt.Sprintf("get wwid from host failed, err: %v", err)
			log.Errorln(msg)
			return "", errors.New(msg)
		}
	}

	if !strings.HasPrefix(data, "naa.") {
		return "", errors.New("unRecognized device type")
	}

	if len(data) < deviceWWidLength {
		msg := fmt.Sprintf("get wwid for device %s failed", hostDevice)
		log.Errorln(msg)
		return "", errors.New(msg)
	}

	return data[deviceWWidLength:], nil
}

// GetSCSIWwn to get the device wwn
func GetSCSIWwn(hostDevice string) (string, error) {
	var wwn string
	var err error
	readable := isDeviceReadable(hostDevice)
	if readable {
		wwn, err = getSCSIWwnByScsiID(hostDevice)
		if err != nil {
			log.Warningf("get device %s wwn by scsi_id error: %v", hostDevice, err)
		}
	} else {
		if strings.HasPrefix(hostDevice, "/dev/sd") {
			wwn, err = getSCSIWwnByWWid(hostDevice)
		}
	}

	return wwn, err
}

// GetNVMeWwn get the unique id of the device
func GetNVMeWwn(device string) (string, error) {
	cmd := fmt.Sprintf("nvme id-ns %s -o json", device)
	output, err := utils.ExecShellCmdFilterLog(cmd)
	if err != nil {
		log.Errorf("Failed to get nvme id of device %s, err is %v", device, err)
		return "", err
	}

	var deviceInfo map[string]interface{}
	if err = json.Unmarshal([]byte(output), &deviceInfo); err != nil {
		log.Errorf("Failed to unmarshal input %s", output)
		return "", errors.New("failed to unmarshal device info")
	}

	if uuid, exist := deviceInfo["nguid"]; exist {
		return uuid.(string), nil
	}

	return "", errors.New("there is no nguid in device info")
}

// ReadDevice is to check whether the device is readable
func ReadDevice(dev string) ([]byte, error) {
	log.Infof("Checking to see if %s is readable.", dev)
	out, err := utils.ExecShellCmdFilterLog("dd if=%s bs=1024 count=512 status=none", dev)
	if err != nil {
		return nil, err
	}

	output := []byte(out)
	if len(output) != halfMiDataLength {
		return nil, fmt.Errorf("can not read 512KiB bytes from the device %s, instead read %d bytes",
			dev, len(output))
	}

	if strings.Contains(out, "0+0 records in") {
		return nil, fmt.Errorf("the size of %s may be zero, it is abnormal device", dev)
	}

	return output, nil
}

// IsDeviceFormatted reads 2MiBs of the device to check the device formatted or not
func IsDeviceFormatted(dev string) (bool, error) {
	output, err := ReadDevice(dev)
	if err != nil {
		return false, err
	}

	// check data is all zero
	if outWithoutZeros := bytes.Trim(output, "\x00"); len(outWithoutZeros) != 0 {
		log.Infof("Device %s is already formatted", dev)
		return true, nil
	}
	log.Infof("Device %s is not formatted", dev)
	return false, nil
}

func removeDevices(devices []string) error {
	for _, dev := range devices {
		err := removeSCSIDevice(dev)
		if err != nil {
			return err
		}
	}
	return nil
}

func removeMultiPathDevice(multiPathName string, devices []string) (string, error) {
	err := FlushDMDevice(multiPathName)
	if err == nil {
		multiPathName = ""
	}

	if err := removeDevices(devices); err != nil {
		return "", err
	}

	waitVolumeRemoval(devices)
	err = removeSCSISymlinks(devices)
	if err != nil {
		return "", err
	}
	return multiPathName, nil
}

func RemoveDevice(device string) (string, error) {
	var multiPathName string
	var err error
	if strings.HasPrefix(device, "dm") {
		devices, _ := getDeviceFromDM(device)
		multiPathName, err = removeMultiPathDevice(device, devices)
	} else if strings.HasPrefix(device, "sd") {
		err = removeSCSIDevice(device)
	} else {
		log.Warningf("Device %s to delete does not exist anymore", device)
	}

	if err != nil {
		return "", err
	}
	return multiPathName, nil
}

// ResizeBlock  Resize a block device by using the LUN WWN
func ResizeBlock(tgtLunWWN string) error {
	var needResizeDM bool
	var devices []string
	device, err := GetDevice(nil, tgtLunWWN, true)
	if err != nil && err.Error() != "FindNoDevice" {
		log.Errorf("Get device of WWN %s error: %v", tgtLunWWN, err)
		return err
	}

	if strings.HasPrefix(device, "dm") {
		devices, err = getDeviceFromDM(device)
		if err != nil {
			log.Errorf("Get device from multiPath %s error: %v", device, err)
			return err
		}

		needResizeDM = true
	} else if strings.HasPrefix(device, "sd") || strings.HasPrefix(device, "nvme") {
		devices = []string{device}
	} else {
		msg := fmt.Sprintf("Device of WWN %s to resize does not exist anymore", tgtLunWWN)
		log.Errorln(msg)
		return errors.New(msg)
	}

	err = extendBlock(devices)
	if err != nil {
		log.Errorf("Extend block %s error: %v", device, err)
		return err
	}

	if needResizeDM {
		err := extendDMBlock(device)
		if err != nil {
			log.Errorf("Extend DM block %s error: %v", device, err)
			return err
		}
	}
	return nil
}

func getDeviceInfo(dev string) map[string]string {
	device := "/dev/" + dev
	output, err := utils.ExecShellCmd("lsblk -n -S %s -o HCTL", device)
	if err != nil {
		log.Warningf("Failed to get device %s hctl", device)
		return nil
	}

	devLines := strings.Split(output, "\n")
	for _, d := range devLines {
		devString := strings.TrimSpace(d)
		hostChannelInfo := strings.Split(devString, ":")
		if len(hostChannelInfo) != intNumFour {
			continue
		}

		devInfo := map[string]string{
			"device":  device,
			"host":    hostChannelInfo[0],
			"channel": hostChannelInfo[1],
			"id":      hostChannelInfo[2],
			"lun":     hostChannelInfo[3],
		}
		return devInfo
	}
	return nil
}

func getDeviceSize(dev string) (string, error) {
	device := "/dev/" + dev
	output, err := utils.ExecShellCmd("blockdev --getsize64 %s", device)
	return output, err
}

func extendBlock(devices []string) error {
	var err error
	for _, dev := range devices {
		if strings.HasPrefix(dev, "sd") {
			err = extendSCSIBlock(dev)
		} else if strings.HasPrefix(dev, "nvme") {
			err = extendNVMeBlock(dev)
		}
	}
	return err
}

func multiPathReconfigure() {
	output, err := utils.ExecShellCmd("multipathd reconfigure")
	if err != nil {
		log.Warningf("Run multipathd reconfigure err. Output: %s, err: %v", output, err)
	}
}

func multiPathResizeMap(device string) (string, error) {
	cmd := fmt.Sprintf("multipathd resize map %s", device)
	output, err := utils.ExecShellCmd(cmd)
	return output, err
}

func extendDMBlock(device string) error {
	multiPathReconfigure()
	oldSize, err := getDeviceSize(device)
	if err != nil {
		return err
	}
	log.Infof("Original size of block %s is %s", device, oldSize)

	time.Sleep(time.Second * 2)
	result, err := multiPathResizeMap(device)
	if err != nil || strings.Contains(result, "fail") {
		msg := fmt.Sprintf("Resize device %s err, output: %s, err: %v", device, result, err)
		log.Errorln(msg)
		return errors.New(msg)
	}

	newSize, err := getDeviceSize(device)
	if err != nil {
		return err
	}
	log.Infof("After scsi device rescan, new size is %s", newSize)
	return nil
}

func extendSCSIBlock(device string) error {
	devInfo := getDeviceInfo(device)
	if devInfo == nil {
		return errors.New("can not get device info")
	}

	oldSize, err := getDeviceSize(device)
	if err != nil {
		return err
	}
	log.Infof("Original size of block %s is %s", device, oldSize)

	_, err = utils.ExecShellCmd("echo 1 > /sys/bus/scsi/drivers/sd/%s:%s:%s:%s/rescan",
		devInfo["host"], devInfo["channel"], devInfo["id"], devInfo["lun"])
	if err != nil {
		return err
	}

	newSize, err := getDeviceSize(device)
	if err != nil {
		return err
	}
	log.Infof("After scsi device rescan, new size is %s", newSize)
	return nil
}

func extendNVMeBlock(device string) error {
	return reScanNVMe(device)
}

// ResizeMountPath  Resize the mount point by using the volume path
func ResizeMountPath(volumePath string) error {
	output, err := utils.ExecShellCmd("findmnt -o source --noheadings --target %s", volumePath)
	if err != nil {
		return fmt.Errorf("findmnt volumePath: %s error: %v", volumePath, err)
	}

	devicePath := strings.TrimSpace(output)
	if len(devicePath) == 0 {
		return fmt.Errorf("could not get valid device for mount path: %s", volumePath)
	}

	fsType, err := utils.ExecShellCmd("blkid -p -s TYPE -o value %s", devicePath)
	if err != nil {
		log.Errorf("blkid %s error: %v", devicePath, err)
		return err
	}

	if fsType == "" {
		return nil
	}

	fsType = strings.Trim(fsType, "\n")
	switch fsType {
	case "ext2", "ext3", "ext4":
		return extResize(devicePath)
	}

	return fmt.Errorf("resize of format %s is not supported for device %s", fsType, devicePath)
}

func extResize(devicePath string) error {
	output, err := utils.ExecShellCmd("resize2fs -p %s", devicePath)
	if err != nil {
		log.Errorf("Resize %s error: %s", devicePath, output)
		return err
	}

	log.Infof("Resize success for device path : %v", devicePath)
	return nil
}

func findMultiPathWWN(mPath string) (string, error) {
	output, err := utils.ExecShellCmd("multipathd show maps")
	if err != nil {
		log.Errorf("Show multipath %s error: %s", mPath, output)
		return "", err
	}

	for _, out := range strings.Split(output, "\n") {
		pathMaps := strings.Fields(out)
		if len(pathMaps) == 3 && pathMaps[1] == mPath {
			return pathMaps[2], nil
		}
	}

	msg := fmt.Sprintf("Path %s not exist in multipath map", mPath)
	log.Errorln(msg)
	return "", errors.New(msg)
}

// Input devices: [sda, sdb, sdc]
func findDeviceWWN(devices []string) (string, error) {
	var findWWN, devWWN string
	var err error
	for _, d := range devices {
		dev := fmt.Sprintf("/dev/%s", d)
		if strings.HasPrefix(d, "sd") {
			devWWN, err = GetSCSIWwn(dev)
		} else if strings.HasPrefix(d, "nvme") {
			devWWN, err = GetNVMeWwn(dev)
		}

		if err != nil {
			log.Warningf("get device %s wwn failed, error: %v", dev, err)
			continue
		}

		if findWWN != "" && !(strings.Contains(devWWN, findWWN) ||
			strings.Contains(findWWN, devWWN)) {
			return "", errors.New("InconsistentWWN")
		}
		findWWN = devWWN
	}

	log.Infof("find the wwn %s for devices %v", findWWN, devices)
	return findWWN, nil
}

func checkDeviceReadable(devices []string) bool {
	for _, dev := range devices {
		_, err := ReadDevice(dev)
		if err != nil {
			log.Errorf("the device %s is not readable", dev)
			return false
		}
	}

	return true
}

func clearFaultyDevices(devices []string) ([]string, error) {
	var normalDevices []string
	for _, d := range devices {
		dev := fmt.Sprintf("/dev/%s", d)
		readable := isDeviceReadable(dev)
		if readable {
			normalDevices = append(normalDevices, d)
			continue
		}

		err := removeSCSIDevice(d)
		if err != nil {
			return nil, err
		}
	}

	return normalDevices, nil
}

// IsMultiPathAvailable compares the dm device WWN with the lun WWN
func IsMultiPathAvailable(mPath, lunWWN string, devices []string) (bool, error) {
	mPathWWN, err := findMultiPathWWN(mPath)
	if err != nil {
		return false, err
	}

	if !strings.Contains(mPathWWN, lunWWN) {
		log.Errorf("the multipath device WWN %s is not equal to lun WWN %s", mPathWWN, lunWWN)
		return false, errors.New("the multipath device WWN is not equal to lun WWN")
	}

	deviceWWN, err := findDeviceWWN(devices)
	if err != nil {
		return false, err
	}

	// false means unavailable when scan device, nil means when delete device without check
	if deviceWWN == "" {
		return false, nil
	}

	if !strings.Contains(deviceWWN, lunWWN) {
		return false, errors.New("the device WWN is not equal to lun WWN")
	}

	return true, nil
}

// IsDeviceAvailable compares the sd device WWN with the lun WWN
func IsDeviceAvailable(device, lunWWN string) (bool, error) {
	var devWWN string
	var err error
	if strings.Contains(device, "sd") {
		devWWN, err = GetSCSIWwn(device)
	} else if strings.Contains(device, "nvme") {
		devWWN, err = GetNVMeWwn(device)
	} else {
		// scsi mode, the device is /dev/disk/by-id/wwn-<id>,
		devWWN, err = GetSCSIWwn(device)
	}

	if err != nil {
		return false, err
	}

	if devWWN == "" {
		return false, nil
	}

	if !strings.Contains(devWWN, lunWWN) {
		return false, errors.New("the device WWN is not equal to lun WWN")
	}
	return true, nil
}

// RealPath obtains the actual device, such as: dm-5
func RealPath(mPath string) (string, error) {
	path, err := filepath.EvalSymlinks(mPath)
	if err != nil {
		return "", err
	}

	splits := strings.Split(path, "/")
	// path such as /dev/sdm or /dev/dm-10, so the splits are ["", "dev", "sdm"] or ["", "dev", "dm-10"]
	if len(splits) == 3 {
		return splits[2], nil
	}
	return "", fmt.Errorf("find the real path failed")
}

// DisConnectVolume delete all devices which match to lunWWN
func DisConnectVolume(tgtLunWWN string, checkDeviceAvailable bool, f func(string, bool) error) error {
	for {
		err := f(tgtLunWWN, checkDeviceAvailable)
		if err != nil {
			if err.Error() == "FindNoDevice" {
				break
			}
			return err
		}
		time.Sleep(time.Second * 2)
	}
	return nil
}

func isDeviceReadable(dev string) bool {
	_, err := ReadDevice(dev)
	if err != nil {
		log.Warningf("Device:%s is unreadable. Error: %v", dev, err)
		return false
	}

	return true
}

// CheckConnectSuccess is to check the sd device available
func CheckConnectSuccess(device, tgtLunWWN string) bool {
	devPath := fmt.Sprintf("/dev/%s", device)
	if readable := isDeviceReadable(devPath); !readable {
		return false
	}

	available, err := IsDeviceAvailable(devPath, tgtLunWWN)
	if err != nil {
		return false
	}

	return available
}

// ClearUnavailableDevice is to check the sd device connect success, otherwise delete the device
func ClearUnavailableDevice(device, lunWWN string) string {
	if !CheckConnectSuccess(device, lunWWN) {
		if err := DeleteSDDev(device); err != nil {
			log.Warningf("clear device %s for lun %s error: %v", device, lunWWN, err)
		}
		device = ""
	}
	return device
}

// VerifySingleDevice check the sd device whether available
func VerifySingleDevice(device, lunWWN, errCode string, checkDeviceAvailable bool,
	f func(string, bool) error) error {
	log.Infof("Found the dev %s", device)
	_, err := ReadDevice(device)
	if err != nil {
		return err
	}

	available, err := IsDeviceAvailable(device, lunWWN)
	if err != nil && err.Error() != "the device WWN is not equal to lun WWN" {
		return err
	}

	if !available {
		err = f(lunWWN, checkDeviceAvailable)
		if err != nil {
			log.Errorf("delete device err while revert connect volume. Err is: %v", err)
		}
		return errors.New(errCode)
	}
	return nil
}

// VerifyMultiPathDevice check the dm device whether available
func VerifyMultiPathDevice(mPath, lunWWN, errCode string, checkDeviceAvailable bool,
	f func(string, bool) error) (string, error) {
	log.Infof("Found the dm path %s", mPath)
	device := fmt.Sprintf("/dev/%s", mPath)
	_, err := ReadDevice(device)
	if err != nil {
		return "", err
	}

	devs, err := getDeviceFromDM(mPath)
	if err != nil {
		return "", err
	}

	devices, err := clearFaultyDevices(devs)
	if err != nil {
		return "", err
	}

	available, err := IsMultiPathAvailable(mPath, lunWWN, devices)
	if err != nil && err.Error() == "InconsistentWWN" {
		return "", err
	}

	if !available {
		err = f(lunWWN, checkDeviceAvailable)
		if err != nil {
			log.Errorf("delete device err while revert connect volume. Err is: %v", err)
		}
		return "", errors.New(errCode)
	}
	return device, nil
}

// RemoveRoCEDevice remove RoCE device or dm device
func RemoveRoCEDevice(device string) ([]string, string, error) {
	var multiPathName string
	var devices []string
	var err error
	if strings.HasPrefix(device, "dm") {
		multiPathName = device
		// devices: nvme0n1, nvme2n1,
		devices, err = getDeviceFromDM(multiPathName)
		if err != nil {
			log.Warningf("get the devices from the multipath %s error: %v", multiPathName, err)
		}

		// just flush the dm path. no need to delete device on host, when delete the storage mapping
		// the device will be automatically deleted.
		err := FlushDMDevice(multiPathName)
		if err == nil {
			multiPathName = ""
		}
	} else if strings.HasPrefix(device, "nvme") {
		devices = append(devices, device)
	} else {
		log.Warningf("NVME Device %s to delete does not exist anymore", device)
	}

	return devices, multiPathName, nil
}

// GetDeviceSize to get the device size in bytes
func GetDeviceSize(hostDevice string) (int64, error) {
	// hostDevice is the symbol, such as /dev/sdb, /dev/dm-5, /dev/mapper/mpatha .etc
	output, err := utils.ExecShellCmd("blockdev --getsize64 %s", hostDevice)
	if err != nil {
		log.Errorf("Failed to get device %s, err is %v", hostDevice, err)
		return 0, err
	}

	outputLines := strings.Split(output, "\n")
	for _, line := range outputLines {
		if line == "" {
			continue
		}
		size, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			log.Errorf("Failed to get device size %s, err is %v", line, err)
			return 0, err
		}
		return size, nil
	}

	return 0, errors.New("failed to get device size")
}

// IsInFormatting is to check the device whether in formatting
func IsInFormatting(sourcePath, fsType string) (bool, error) {
	var cmd string
	if fsType != "ext2" && fsType != "ext3" && fsType != "ext4" {
		msg := fmt.Sprintf("Do not support the type %s.", fsType)
		log.Errorln(msg)
		return false, errors.New(msg)
	}

	cmd = fmt.Sprintf("ps -aux | grep mkfs | grep -w %s | wc -l |awk '{if($1>1) print 1; else print 0}'",
		sourcePath)
	output, err := utils.ExecShellCmd(cmd)
	if err != nil {
		return false, err
	}

	outputSplit := strings.Split(output, "\n")
	return len(outputSplit) != 0 && outputSplit[0] == "1", nil
}
