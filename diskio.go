package lotusdb

import (
	"errors"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/disk"
)

const IOBusySplit = 2

type DiskIO struct {
	targetPath       string
	samplingInterval int     // unit millisecond
	IOBusyThreshold  uint64  // rate, read/write bytes during samplingInterval > readBusyThreshold is busy
	busyRate         float32 // express io busy status by the proportion of io time in the sampling time
	freeFlag         bool    // freeFlag indicates whether the disk is free
	mu               sync.Mutex
}

func (io *DiskIO) Monitor() error {
	var ioStart disk.IOCountersStat
	var ioEnd disk.IOCountersStat
	var err error

	ioStart, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}

	time.Sleep(time.Duration(io.samplingInterval) * time.Millisecond)

	ioEnd, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}
	ioTime := ioEnd.IoTime - ioStart.IoTime
	io.mu.Lock()
	defer io.mu.Unlock()
	// if ioTime > 0 {
	// 	log.Println("ioTime:",ioTime,"threshold",uint64(float32(io.samplingInterval) * io.busyRate))
	// }
	if ioTime > uint64(float32(io.samplingInterval)*io.busyRate) {
		io.freeFlag = false
	} else {
		io.freeFlag = true
	}
	return nil
}

func (io *DiskIO) IsFree() (bool, error) {
	if runtime.GOOS != "linux" {
		return true, nil
	}
	if io.busyRate < 0 {
		return true, nil
	}
	io.mu.Lock()
	defer io.mu.Unlock()
	return io.freeFlag, nil
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
