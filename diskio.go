package lotusdb

import (
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/disk"
)

type DiskIO struct {
	targetPath       string   // db path, we use it to find disk device
	samplingInterval int      // sampling time, millisecond
	samplingWindow   []uint64 // sampling window is used to sample the average IoTime over a period of time
	windowSize       int      // size of the sliding window used for sampling.
	windowPoint      int      // next sampling offset in window
	busyRate         float32  // express io busy status by the proportion of io time in the sampling time
	freeFlag         bool     // freeFlag indicates whether the disk is free
	mu               sync.Mutex
}

func (io *DiskIO) Init() {
	io.samplingWindow = make([]uint64, io.windowSize)
	io.windowPoint = 0
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

	// IoTime is device active time since system boot, we get it during sampling.
	ioTime := ioEnd.IoTime - ioStart.IoTime

	// set time and move point to next slot
	io.samplingWindow[io.windowPoint] = ioTime
	io.windowPoint++
	io.windowPoint %= io.windowSize

	// get mean IoTime
	var sum uint64
	for _, value := range io.samplingWindow {
		sum += value
	}
	meanTime := sum / (uint64(io.windowSize))

	// others may read io.freeFlag by IsFree, so we need lock it when changing.
	io.mu.Lock()
	defer io.mu.Unlock()
	// this log maybe useful
	// log.Println("meantime:", meanTime, "BusyThreshold:", uint64(float32(io.samplingInterval)*io.busyRate))
	if meanTime > uint64(float32(io.samplingInterval)*io.busyRate) {
		io.freeFlag = false
	} else {
		io.freeFlag = true
	}
	return nil
}

func (io *DiskIO) IsFree() (bool, error) {
	// if runtime.GOOS != "linux" {
	// 	return true, nil
	// }
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
