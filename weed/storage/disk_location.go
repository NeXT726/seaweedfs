package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type DiskLocation struct {
	Directory              string
	IdxDirectory           string
	DiskType               types.DiskType
	MaxVolumeCount         int
	OriginalMaxVolumeCount int
	MinFreeSpace           util.MinFreeSpace
	volumes                map[needle.VolumeId]*Volume
	volumesLock            sync.RWMutex

	// erasure coding
	ecVolumes     map[needle.VolumeId]*erasure_coding.EcVolume
	ecVolumesLock sync.RWMutex

	isDiskSpaceLow bool
}

func NewDiskLocation(dir string, maxVolumeCount int, minFreeSpace util.MinFreeSpace, idxDir string, diskType types.DiskType) *DiskLocation {
	dir = util.ResolvePath(dir)
	if idxDir == "" {
		idxDir = dir
	} else {
		idxDir = util.ResolvePath(idxDir)
	}
	location := &DiskLocation{
		Directory:              dir,
		IdxDirectory:           idxDir,
		DiskType:               diskType,
		MaxVolumeCount:         maxVolumeCount,
		OriginalMaxVolumeCount: maxVolumeCount,
		MinFreeSpace:           minFreeSpace,
	}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	go location.CheckDiskSpace()
	return location
}

func volumeIdFromFileName(filename string) (needle.VolumeId, string, error) {
	if isValidVolume(filename) {
		base := filename[:len(filename)-4]
		collection, volumeId, err := parseCollectionVolumeId(base)
		return volumeId, collection, err
	}

	return 0, "", fmt.Errorf("file is not a volume: %s", filename)
}

// volume文件名结构为： collection_vid.idx/collection_vid.vif
// 以'_'为分隔符把collection和vid解析出来即可
// vid要通过转化，把string转化为uint返回
func parseCollectionVolumeId(base string) (collection string, vid needle.VolumeId, err error) {
	i := strings.LastIndex(base, "_")
	if i > 0 {
		collection, base = base[0:i], base[i+1:]
	}
	vol, err := needle.NewVolumeId(base)
	return collection, vol, err
}

func isValidVolume(basename string) bool {
	return strings.HasSuffix(basename, ".idx") || strings.HasSuffix(basename, ".vif")
}

func getValidVolumeName(basename string) string {
	if isValidVolume(basename) {
		return basename[:len(basename)-4]
	}
	return ""
}

func (l *DiskLocation) loadExistingVolume(dirEntry os.DirEntry, needleMapKind NeedleMapKind, skipIfEcVolumesExists bool) bool {
	basename := dirEntry.Name()
	if dirEntry.IsDir() {
		return false
	}
	volumeName := getValidVolumeName(basename)
	if volumeName == "" {
		return false
	}

	// skip if ec volumes exists
	if skipIfEcVolumesExists {
		//ec volumes文件以.ecx结尾
		if util.FileExists(l.Directory + "/" + volumeName + ".ecx") {
			return false
		}
	}

	// check for incomplete volume
	// 当存在.note结尾的文件，就是说该volume是不完整的，所以就都删除掉（其中，.note中记录的一些log
	noteFile := l.Directory + "/" + volumeName + ".note"
	if util.FileExists(noteFile) {
		note, _ := os.ReadFile(noteFile)
		glog.Warningf("volume %s was not completed: %s", volumeName, string(note))
		removeVolumeFiles(l.Directory + "/" + volumeName)
		removeVolumeFiles(l.IdxDirectory + "/" + volumeName)
		return false
	}

	// parse out collection, volume id
	// 从volume的文件名中把collection和vid解析出来
	vid, collection, err := volumeIdFromFileName(basename)
	if err != nil {
		glog.Warningf("get volume id failed, %s, err : %s", volumeName, err)
		return false
	}

	// avoid loading one volume more than once
	l.volumesLock.RLock()
	_, found := l.volumes[vid]
	l.volumesLock.RUnlock()
	if found {
		glog.V(1).Infof("loaded volume, %v", vid)
		return true
	}

	// load the volume
	// DJLTODO
	v, e := NewVolume(l.Directory, l.IdxDirectory, collection, vid, needleMapKind, nil, nil, 0, 0)
	if e != nil {
		glog.V(0).Infof("new volume %s error %s", volumeName, e)
		return false
	}

	//把这个已有的volume存放在DiskLocation的volume映射关系中
	l.SetVolume(vid, v)

	size, _, _ := v.FileStat()
	glog.V(0).Infof("data file %s, replication=%s v=%d size=%d ttl=%s",
		l.Directory+"/"+volumeName+".dat", v.ReplicaPlacement, v.Version(), size, v.Ttl.String())
	return true
}

func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapKind, concurrency int) {

	task_queue := make(chan os.DirEntry, 10*concurrency)
	go func() {
		foundVolumeNames := make(map[string]bool)
		//对location目录下的所有项，找到.idx/.vif后缀的文件，作为本来已经存在的volume保存
		if dirEntries, err := os.ReadDir(l.Directory); err == nil {
			for _, entry := range dirEntries {
				//对有.idx和.vif后缀的文件视为已有的volume，并去掉这四个字符的后缀后返回volumename
				volumeName := getValidVolumeName(entry.Name())
				if volumeName == "" {
					continue
				}
				if _, found := foundVolumeNames[volumeName]; !found {
					foundVolumeNames[volumeName] = true
					task_queue <- entry
				}
			}
		}
		close(task_queue)
	}()

	var wg sync.WaitGroup
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range task_queue {
				_ = l.loadExistingVolume(fi, needleMapKind, true)
			}
		}()
	}
	wg.Wait()

}

func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapKind) {

	l.concurrentLoadingVolumes(needleMapKind, 10)
	glog.V(0).Infof("Store started on dir: %s with %d volumes max %d", l.Directory, len(l.volumes), l.MaxVolumeCount)

	l.loadAllEcShards()
	glog.V(0).Infof("Store started on dir: %s with %d ec shards", l.Directory, len(l.ecVolumes))

}

func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {

	l.volumesLock.Lock()
	delVolsMap := l.unmountVolumeByCollection(collection)
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	delEcVolsMap := l.unmountEcVolumeByCollection(collection)
	l.ecVolumesLock.Unlock()

	errChain := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for _, v := range delVolsMap {
			if err := v.Destroy(); err != nil {
				errChain <- err
			}
		}
		wg.Done()
	}()

	go func() {
		for _, v := range delEcVolsMap {
			v.Destroy()
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(errChain)
	}()

	errBuilder := strings.Builder{}
	for err := range errChain {
		errBuilder.WriteString(err.Error())
		errBuilder.WriteString("; ")
	}
	if errBuilder.Len() > 0 {
		e = fmt.Errorf(errBuilder.String())
	}

	return
}

func (l *DiskLocation) deleteVolumeById(vid needle.VolumeId) (found bool, e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy()
	if e != nil {
		return
	}
	found = true
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) LoadVolume(vid needle.VolumeId, needleMapKind NeedleMapKind) bool {
	if fileInfo, found := l.LocateVolume(vid); found {
		return l.loadExistingVolume(fileInfo, needleMapKind, false)
	}
	return false
}

var ErrVolumeNotFound = fmt.Errorf("volume not found")

func (l *DiskLocation) DeleteVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return ErrVolumeNotFound
	}
	_, err := l.deleteVolumeById(vid)
	return err
}

func (l *DiskLocation) UnloadVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return ErrVolumeNotFound
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

func (l *DiskLocation) unmountVolumeByCollection(collectionName string) map[needle.VolumeId]*Volume {
	deltaVols := make(map[needle.VolumeId]*Volume, 0)
	for k, v := range l.volumes {
		if v.Collection == collectionName && !v.isCompacting {
			deltaVols[k] = v
		}
	}

	for k := range deltaVols {
		delete(l.volumes, k)
	}
	return deltaVols
}

func (l *DiskLocation) SetVolume(vid needle.VolumeId, volume *Volume) {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	l.volumes[vid] = volume
	volume.location = l
}

func (l *DiskLocation) FindVolume(vid needle.VolumeId) (*Volume, bool) {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

func (l *DiskLocation) VolumesLen() int {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	return len(l.volumes)
}

func (l *DiskLocation) Close() {
	l.volumesLock.Lock()
	for _, v := range l.volumes {
		v.Close()
	}
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	for _, ecVolume := range l.ecVolumes {
		ecVolume.Close()
	}
	l.ecVolumesLock.Unlock()

	return
}

func (l *DiskLocation) LocateVolume(vid needle.VolumeId) (os.DirEntry, bool) {
	// println("LocateVolume", vid, "on", l.Directory)
	if dirEntries, err := os.ReadDir(l.Directory); err == nil {
		for _, entry := range dirEntries {
			// println("checking", entry.Name(), "...")
			volId, _, err := volumeIdFromFileName(entry.Name())
			// println("volId", volId, "err", err)
			if vid == volId && err == nil {
				return entry, true
			}
		}
	}

	return nil, false
}

func (l *DiskLocation) UnUsedSpace(volumeSizeLimit uint64) (unUsedSpace uint64) {

	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	for _, vol := range l.volumes {
		if vol.IsReadOnly() {
			continue
		}
		datSize, idxSize, _ := vol.FileStat()
		unUsedSpace += volumeSizeLimit - (datSize + idxSize)
	}

	return
}

func (l *DiskLocation) CheckDiskSpace() {
	for {
		if dir, e := filepath.Abs(l.Directory); e == nil {
			s := stats.NewDiskStatus(dir)
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "all").Set(float64(s.All))
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "used").Set(float64(s.Used))
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "free").Set(float64(s.Free))

			isLow, desc := l.MinFreeSpace.IsLow(s.Free, s.PercentFree)
			if isLow != l.isDiskSpaceLow {
				l.isDiskSpaceLow = !l.isDiskSpaceLow
			}

			logLevel := glog.Level(4)
			if l.isDiskSpaceLow {
				logLevel = glog.Level(0)
			}

			glog.V(logLevel).Infof("dir %s %s", dir, desc)
		}
		time.Sleep(time.Minute)
	}

}
