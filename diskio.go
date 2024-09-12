package lotusdb

import (
	"errors"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/shirou/gopsutil/disk"
)

const IOBusySplit = 2

type DiskIO struct {
	targetPath         string
	samplingInterval   int                 // unit millisecond
	IOBusyThreshold    uint64              // rate, read/write bytes during samplingInterval > readBusyThreshold is busy
	collectTimeStamp   time.Time           // used for collecting time in flushmemtable, and get Bandwidth
	collectIOStat      disk.IOCountersStat // used for collecting io msg in flushmemtable, and get Bandwidth
}

func (io *DiskIO) IsFree() (bool, error) {
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
	log.Println("IOThreshold:", io.IOBusyThreshold, "IOBytes:", readBytes+writeBytes)

	if io.IOBusyThreshold < readBytes+writeBytes {
		return false, nil
	}

	return true, nil
}

func (io *DiskIO) BandwidthCollectStart() error {
	io.collectTimeStamp = time.Now()
	var err error
	io.collectIOStat, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}
	return nil
}

func (io *DiskIO) BandwidthCollectEnd() error {
	endCollTimeStamp := time.Now()
	endCollectIOStat, err := GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}
	duration := endCollTimeStamp.Sub(io.collectTimeStamp)
	ms := uint64(duration.Milliseconds())
	readBytes := endCollectIOStat.ReadBytes - io.collectIOStat.ReadBytes
	writeBytes := endCollectIOStat.WriteBytes - io.collectIOStat.WriteBytes
	newIOBusyThreshold := ((readBytes + writeBytes)/ IOBusySplit) / ms
	if newIOBusyThreshold > io.IOBusyThreshold {
		io.IOBusyThreshold = newIOBusyThreshold
	}
	log.Println("Update IOBusyThreshold:", io.IOBusyThreshold)
	return nil
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
