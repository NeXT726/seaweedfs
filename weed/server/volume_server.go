package weed_server

import (
	"net/http"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/types"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type VolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	inFlightUploadDataSize        int64
	inFlightDownloadDataSize      int64
	concurrentUploadLimit         int64
	concurrentDownloadLimit       int64
	inFlightUploadDataLimitCond   *sync.Cond
	inFlightDownloadDataLimitCond *sync.Cond

	SeedMasterNodes []pb.ServerAddress
	currentMaster   pb.ServerAddress
	pulseSeconds    int
	dataCenter      string
	rack            string
	store           *storage.Store
	guard           *security.Guard
	grpcDialOption  grpc.DialOption

	needleMapKind           storage.NeedleMapKind
	FixJpgOrientation       bool
	ReadMode                string
	compactionBytePerSecond int64
	metricsAddress          string
	metricsIntervalSec      int
	fileSizeLimitBytes      int64
	isHeartbeating          bool
	stopChan                chan bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, grpcPort int, publicUrl string,
	folders []string, maxCounts []int, minFreeSpaces []util.MinFreeSpace, diskTypes []types.DiskType,
	idxFolder string,
	needleMapKind storage.NeedleMapKind,
	masterNodes []pb.ServerAddress, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readMode string,
	compactionMBPerSecond int,
	fileSizeLimitMB int,
	concurrentUploadLimit int64,
	concurrentDownloadLimit int64,
) *VolumeServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
	enableUiAccess := v.GetBool("access.ui")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		needleMapKind:     needleMapKind,
		FixJpgOrientation: fixJpgOrientation,
		//readMode 默认是proxy
		ReadMode:                      readMode,
		grpcDialOption:                security.LoadClientTLS(util.GetViper(), "grpc.volume"),
		compactionBytePerSecond:       int64(compactionMBPerSecond) * 1024 * 1024,
		fileSizeLimitBytes:            int64(fileSizeLimitMB) * 1024 * 1024,
		isHeartbeating:                true,
		stopChan:                      make(chan bool),
		inFlightUploadDataLimitCond:   sync.NewCond(new(sync.Mutex)),
		inFlightDownloadDataLimitCond: sync.NewCond(new(sync.Mutex)),
		concurrentUploadLimit:         concurrentUploadLimit,
		concurrentDownloadLimit:       concurrentDownloadLimit,
	}
	vs.SeedMasterNodes = masterNodes

	vs.checkWithMaster()

	vs.store = storage.NewStore(vs.grpcDialOption, ip, port, grpcPort, publicUrl, folders, maxCounts, minFreeSpaces, idxFolder, vs.needleMapKind, diskTypes)
	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	handleStaticResources(adminMux)
	adminMux.HandleFunc("/status", vs.statusHandler)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		/*
			adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
			adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
			adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
		*/
	}
	//这个处理函数是volume的http服务器应对读写的主要入口
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go vs.heartbeat()
	go stats.LoopPushingMetric("volumeServer", util.JoinHostPort(ip, port), vs.metricsAddress, vs.metricsIntervalSec)

	return vs
}

func (vs *VolumeServer) SetStopping() {
	glog.V(0).Infoln("Stopping volume server...")
	vs.store.SetStopping()
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}
