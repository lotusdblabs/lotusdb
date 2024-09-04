package lotusdb

import (
	"errors"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/shirou/gopsutil/disk"
)

type DiskIO struct {
	targetPath         string
	samplingInterval   int    // unit millisecond
	readBusyThreshold  uint64 // read bytes during samplingInterval > readBusyThreshold is busy
	writeBusyThreshold uint64 // write bytes during samplingInterval > writeBusyThreshold is busy
}

func (io DiskIO) IsFree() (bool, error) {
	var ioStart disk.IOCountersStat
	var ioEnd disk.IOCountersStat
	var err error
	ioStart, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return false, err
	}
	time.Sleep(time.Duration(io.samplingInterval) * time.Millisecond)
	ioEnd, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return false, err
	}
	readBytes := ioEnd.ReadBytes - ioStart.ReadBytes
	writeBytes := ioEnd.WriteBytes - ioStart.WriteBytes
	log.Println("RdThreshold:", io.readBusyThreshold, "readBytes:", readBytes,
		"WtThreshold:", io.writeBusyThreshold, "writeBytes:", writeBytes)
	if io.readBusyThreshold < readBytes || io.writeBusyThreshold < writeBytes {
		return false, nil
	}
	return true, nil
}

func GetDiskIOInfo(targetPath string) (disk.IOCountersStat, error) {
	var io disk.IOCountersStat
	// Get all mounting points
	partitions, err := disk.Partitions(false)
	if err != nil {
		log.Println("Error getting partitions:", err)
		return io, err
	}

	var targetDevice string

	// Find the mount point where the target path is located
	for _, partition := range partitions {
		if isPathOnDevice(targetPath, partition.Mountpoint) {
			targetDevice = partition.Device
			break
		}
	}

	targetDevice = getDeviceName(targetDevice)

	// Get the I/O status of the device
	ioCounters, err := disk.IOCounters()
	if err != nil {
		return io, err
	}

	var exists bool
	if io, exists = ioCounters[targetDevice]; !exists {
		return io, errors.New("No I/O stats available for device" + targetDevice)
	}
	return io, nil
}

// Check if the path is on the specified mount point.
func getDeviceName(devicePath string) string {
	parts := strings.Split(devicePath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return devicePath
}

// Check if the path is on the specified mount point.
func isPathOnDevice(path, mountpoint string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Println("Error getting absolute path:", err)
		return false
	}

	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		log.Println("Error getting absolute mountpoint:", err)
		return false
	}

	// Ensure paths are normalized for comparison
	absPath = filepath.Clean(absPath)
	absMountpoint = filepath.Clean(absMountpoint)

	return strings.HasPrefix(absPath, absMountpoint)
}
