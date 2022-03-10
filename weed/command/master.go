package command

import (
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/chrislusf/raft/protobuf"
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/reflection"

	"github.com/chrislusf/seaweedfs/weed/util/grace"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/util"
)

//全局变量 m，表示Master的一系列参数
var (
	m MasterOptions
)

type MasterOptions struct {
	port              *int
	portGrpc          *int
	ip                *string
	ipBind            *string
	metaFolder        *string
	peers             *string
	volumeSizeLimitMB *uint
	volumePreallocate *bool
	// pulseSeconds       *int
	defaultReplication *string
	garbageThreshold   *float64
	whiteList          *string
	disableHttp        *bool
	metricsAddress     *string
	metricsIntervalSec *int
	raftResumeState    *bool
	metricsHttpPort    *int
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 9333, "http listen port")
	m.portGrpc = cmdMaster.Flag.Int("port.grpc", 0, "grpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address, also used as identifier")
	m.ipBind = cmdMaster.Flag.String("ip.bind", "", "ip address to bind to")
	//默认把meta data保存在系统tmp目录下，在linux中若$TMPDIR非空则为$TMPDIR，否则为/tmp目录
	m.metaFolder = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	//邻居节点：以逗号分隔的ip:port格式
	m.peers = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
	m.volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	m.volumePreallocate = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	// m.pulseSeconds = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	m.defaultReplication = cmdMaster.Flag.String("defaultReplication", "", "Default replication type if not specified.")
	m.garbageThreshold = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	//逗号分隔有写入权限的IP，如果为空则无限制
	m.whiteList = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	m.disableHttp = cmdMaster.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	m.metricsAddress = cmdMaster.Flag.String("metrics.address", "", "Prometheus gateway address <host>:<port>")
	m.metricsIntervalSec = cmdMaster.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	m.metricsHttpPort = cmdMaster.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	m.raftResumeState = cmdMaster.Flag.Bool("resumeState", false, "resume previous state on start master server")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service and sequence number of file ids

	The configuration file "security.toml" is read from ".", "$HOME/.seaweedfs/", "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order.

	The example security.toml configuration file can be generated by "weed scaffold -config=security"

  `,
}

var (
	masterCpuProfile = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	masterMemProfile = cmdMaster.Flag.String("memprofile", "", "memory profile output file")
)

func runMaster(cmd *Command, args []string) bool {

	//读取security.toml配置文件
	util.LoadConfiguration("security", false)
	//读取master.toml配置文件
	util.LoadConfiguration("master", false)

	//CPU、memory 配置信息
	grace.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	//创建文件夹存储元数据（meta data）
	parent, _ := util.FullPath(*m.metaFolder).DirAndName()
	if util.FileExists(string(parent)) && !util.FileExists(*m.metaFolder) {
		os.MkdirAll(*m.metaFolder, 0755)
	}
	if err := util.TestFolderWritable(util.ResolvePath(*m.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *m.metaFolder, err)
	}

	//白名单，默认为空
	var masterWhiteList []string
	if *m.whiteList != "" {
		masterWhiteList = strings.Split(*m.whiteList, ",")
	}
	//volume size 限制
	if *m.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	//启动一个协程进行状态监测
	go stats_collect.StartMetricsServer(*m.metricsHttpPort)
	//启动Master的主函数
	startMaster(m, masterWhiteList)

	return true
}

func startMaster(masterOption MasterOptions, masterWhiteList []string) {

	backend.LoadConfiguration(util.GetViper())

	//GRPC端口设置
	if *masterOption.portGrpc == 0 {
		*masterOption.portGrpc = 10000 + *masterOption.port
	}

	//用string保存masterAddress，用[]string切片保存peer
	myMasterAddress, peers := checkPeers(*masterOption.ip, *masterOption.port, *masterOption.portGrpc, *masterOption.peers)

	//请求路由和分发函数
	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, masterOption.toMasterOption(masterWhiteList), peers)
	//监听请求的ip地址
	//默认情况下，一开始 ipBind 确实是空的，后面运行过程中会自动补成当前机器的ip
	listeningAddress := util.JoinHostPort(*masterOption.ipBind, *masterOption.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", util.Version(), listeningAddress)
	masterListener, e := util.NewListener(listeningAddress, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}
	// start raftServer
	raftServer, err := weed_server.NewRaftServer(security.LoadClientTLS(util.GetViper(), "grpc.master"),
		peers, myMasterAddress, util.ResolvePath(*masterOption.metaFolder), ms.Topo, *masterOption.raftResumeState)
	if raftServer == nil {
		glog.Fatalf("please verify %s is writable, see https://github.com/chrislusf/seaweedfs/issues/717: %s", *masterOption.metaFolder, err)
	}
	ms.SetRaftServer(raftServer)
	//对url为 masterIP/cluster/status 的Get请求执行raftServer.StatusHandler函数
	r.HandleFunc("/cluster/status", raftServer.StatusHandler).Methods("GET")
	// starting grpc server
	grpcPort := *masterOption.portGrpc
	//ipbind为空，则监听系统的所有ip地址
	grpcL, err := util.NewListener(util.JoinHostPort(*masterOption.ipBind, grpcPort), 0)
	if err != nil {
		glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	master_pb.RegisterSeaweedServer(grpcS, ms)
	protobuf.RegisterRaftServer(grpcS, raftServer)
	reflection.Register(grpcS)
	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.Version(), *masterOption.ipBind, grpcPort)
	go grpcS.Serve(grpcL)

	go func() {
		time.Sleep(1500 * time.Millisecond)
		if ms.Topo.RaftServer.Leader() == "" && ms.Topo.RaftServer.IsLogEmpty() && isTheFirstOne(myMasterAddress, peers) {
			if ms.MasterClient.FindLeaderFromOtherPeers(myMasterAddress) == "" {
				raftServer.DoJoinCommand()
			}
		}
	}()

	go ms.MasterClient.KeepConnectedToMaster()

	// start http server
	httpS := &http.Server{Handler: r}
	go httpS.Serve(masterListener)

	select {}
}

func checkPeers(masterIp string, masterPort int, masterGrpcPort int, peers string) (masterAddress pb.ServerAddress, cleanedPeers []pb.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	masterAddress = pb.NewServerAddress(masterIp, masterPort, masterGrpcPort)
	//将之前以逗号分隔的peer列表切分开，创建新的切片一一保存
	cleanedPeers = pb.ServerAddresses(peers).ToAddresses()

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	//自身也会被加在peer列表中
	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	//master的数量只能是奇数
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}

func isTheFirstOne(self pb.ServerAddress, peers []pb.ServerAddress) bool {
	sort.Slice(peers, func(i, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	if len(peers) <= 0 {
		return true
	}
	return self == peers[0]
}

func (m *MasterOptions) toMasterOption(whiteList []string) *weed_server.MasterOption {
	masterAddress := pb.NewServerAddress(*m.ip, *m.port, *m.portGrpc)
	return &weed_server.MasterOption{
		Master:            masterAddress,
		MetaFolder:        *m.metaFolder,
		VolumeSizeLimitMB: uint32(*m.volumeSizeLimitMB),
		VolumePreallocate: *m.volumePreallocate,
		// PulseSeconds:            *m.pulseSeconds,
		DefaultReplicaPlacement: *m.defaultReplication,
		GarbageThreshold:        *m.garbageThreshold,
		WhiteList:               whiteList,
		DisableHttp:             *m.disableHttp,
		MetricsAddress:          *m.metricsAddress,
		MetricsIntervalSec:      *m.metricsIntervalSec,
	}
}
