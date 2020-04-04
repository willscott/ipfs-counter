package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	libgeo "github.com/nranchev/go-libGeoIP"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	ds "github.com/ipfs/go-datastore"
	ipfsaddr "github.com/ipfs/go-ipfs-addr"
	"github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	mh "github.com/multiformats/go-multihash"
)

var (
	node_counts_g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "nodes_total",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "total number of dht nodes encountered",
	}, []string{"version", "cc"})

	node_connectable_g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "nodes_connectable",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "connectable dht nodes encountered",
	}, []string{"version", "cc"})

	query_lat_h = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "query",
		Subsystem: "dht",
		Namespace: "libp2p",
		Help:      "dht 'findclosestpeers' latencies",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1, 2, 5, 10, 15, 20, 25, 30, 60},
	})

	connect_lat_h = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "connect",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "h.Connect latencies",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1, 2, 5, 10, 15, 20, 25, 30, 60},
	})

	addr_counts_s = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:      "addr_counts",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "address counts discovered by the dht crawls",
	})
)

func init() {
	prometheus.MustRegister(node_counts_g)
	prometheus.MustRegister(node_connectable_g)
	prometheus.MustRegister(query_lat_h)
	prometheus.MustRegister(connect_lat_h)
	prometheus.MustRegister(addr_counts_s)
}

var log = logging.Logger("dht_scrape")

var bspi []pstore.PeerInfo

var DefaultBootstrapAddresses = []string{
	"/dns4/node0.preload.ipfs.io/tcp/4001/p2p/QmZMxNdpMkewiVZLMRxaNxUeZpDUb34pWjZ1kZvsd16Zic",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

func init() {
	for _, a := range DefaultBootstrapAddresses {
		ia, err := ipfsaddr.ParseString(a)
		if err != nil {
			panic(err)
		}

		bspi = append(bspi, pstore.PeerInfo{
			ID:    ia.ID(),
			Addrs: []ma.Multiaddr{ia.Transport()},
		})
	}
}

func main() {
	geoDefault := "/usr/share/GeoIP/"
	if os.Getenv("GEO_PATH") != "" {
		geoDefault = os.Getenv("GEO_PATH")
	}
	geo4, err := libgeo.Load(geoDefault + "GeoIP.dat")
	if err != nil {
		log.Error("country detection unvailable. set GEO_PATH to configure.")
	}

	if err := buildHostAndScrapePeers(geo4); err != nil {
		log.Error("scrape failed: ", err)
	}

	if os.Getenv("PUSHGATEWAY_BASIC_AUTH") != "" {
		user := strings.SplitN(os.Getenv("PUSHGATEWAY_BASIC_AUTH"), ":", 2)
		push.New(os.Getenv("PUSHGATEWAY"), "content_routing_live").Gatherer(prometheus.DefaultGatherer).BasicAuth(user[0], user[1]).Push()
	} else {
		metrics, _ := prometheus.DefaultGatherer.Gather()
		for _, m := range metrics {
			fmt.Printf("%s: %s\n", m.Name, m.String())
		}
	}
}

type trackingInfo struct {
	Addresses    []ma.Multiaddr `json:"a"`
	AgentVersion string         `json:"av"`
	Connected    bool           `json:"c"`
}

func buildHostAndScrapePeers(lg *libgeo.GeoIP) error {
	fmt.Println("building new node to collect metrics with")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/4001"))
	if err != nil {
		return err
	}

	defer func() {
		fmt.Println("closing host...")
		h.Close()
	}()

	bootstrap(ctx, h)
	fmt.Printf("connected bootstrap:%v\n", h.Network().Peers())

	mds := ds.NewMapDatastore()
	mdht, err := dht.New(ctx, h,
		dht.Datastore(mds),
		dht.Mode(dht.ModeClient),
		dht.RoutingTableRefreshPeriod(time.Minute),
		dht.Validator(record.NamespacedValidator{
			"pk":   record.PublicKeyValidator{},
			"ipns": ipns.Validator{KeyBook: h.Peerstore()}}))
	if err != nil {
		panic(err)
	}

	knownPeers := make(map[peer.ID]*trackingInfo)
	seen := 0
	newSeen := 0
	kpl := sync.Mutex{}
	ectx, evch := dht.RegisterForLookupEvents(ctx)
	go func() {
		for {
			select {
			case <-ectx.Done():
				return
			case le, ok := <-evch:
				if !ok {
					return
				}
				kpl.Lock()
				if le.Response == nil || le.Response.Heard == nil {
					kpl.Unlock()
					continue
				}
				fmt.Printf(",")
				for _, heardPeer := range le.Response.Heard {
					seen++
					t, ok := knownPeers[heardPeer.Peer]
					if !ok {
						newSeen++
						t = new(trackingInfo)
						knownPeers[heardPeer.Peer] = t
					}
				}
				kpl.Unlock()
			}
		}
	}()

	prevSeen := 0
	prevNew := 0
	for {
		pokeDHT(ectx, mdht)
		fmt.Printf("\nconnected to %d peers. in last iteration, %d new peers seen of %d heard of.\n",
			len(h.Network().Peers()),
			newSeen-prevNew,
			seen-prevSeen)
		kpl.Lock()
		// go until a process of poking 1000 peers results in finding less than 100 new ones.
		if seen-prevSeen > 100 {
			fmt.Printf("new rate is %f\n", float64(newSeen-prevNew)/float64(seen-prevSeen))
			if float64(newSeen-prevNew)/float64(seen-prevSeen) < 0.05 {
				kpl.Unlock()
				break
			}
			prevSeen = seen
			prevNew = newSeen
		}
		kpl.Unlock()
	}

	fmt.Printf("through the DHT, learned about %d distinct peers\n", newSeen)
	if err := scrapePeers(h, mdht, knownPeers, lg); err != nil {
		return err
	}
	return nil
}

func pokeDHT(ctx context.Context, mdht *dht.IpfsDHT) bool {
	wg := sync.WaitGroup{}
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go scrapeRound(ctx, &wg, mdht, getRandomString())
	}
	wg.Wait()
	return true
}

func getRandomString() string {
	buf := make([]byte, 32)
	rand.Read(buf)
	o, err := mh.Encode(buf, mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	return string(o)
}

// bootstrap (TODO: choose from larger group of peers)
func bootstrap(ctx context.Context, h host.Host) {
	var wg sync.WaitGroup
	for i := 0; i < len(bspi); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := len(bspi) - (1 + i)
			if err := h.Connect(ctx, bspi[v]); err != nil {
				log.Error(bspi[v], err)
			}
		}(i)
	}
	wg.Wait()
}

func has(s string, l []string) bool {
	for _, m := range l {
		if s == m {
			return true
		}
	}
	return false
}

func scrapeRound(ctx context.Context, wg *sync.WaitGroup, mdht *dht.IpfsDHT, k string) {
	mctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	defer wg.Done()
	defer fmt.Print(".")

	start := time.Now()
	peers, err := mdht.GetClosestPeers(mctx, k)
	if err != nil {
		log.Debug(err)
		return
	}

	done := false
	for !done {
		select {
		case _, ok := <-peers:
			if !ok {
				done = true
			}
		case <-mctx.Done():
			done = true
		}
	}
	took := time.Since(start).Seconds()
	query_lat_h.Observe(took)
}

func peerFinder(ctx context.Context, dht *dht.IpfsDHT, ids chan discoverMsg, infos chan discoverMsg, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-ids:
			if !ok {
				return
			}
			cctx, cncl := context.WithTimeout(ctx, time.Second*30)
			start := time.Now()
			info, err := dht.FindPeer(cctx, p.ID)
			took := time.Since(start).Seconds()
			connect_lat_h.Observe(took)
			cncl()
			if err == nil {
				p.Addresses = info.Addrs
				infos <- p
			}
		}
	}
}

func connector(ctx context.Context, h host.Host, infos chan discoverMsg, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case i, ok := <-infos:
			if !ok {
				return
			}
			if len(i.Addresses) == 0 {
				continue
			}
			cctx, cncl := context.WithTimeout(ctx, time.Second*30)
			err := h.Connect(cctx, peer.AddrInfo{ID: i.ID, Addrs: i.Addresses})
			if err != nil {
				cncl()
				continue
			}
			i.Connected = true
			av, _ := h.Peerstore().Get(i.ID, "AgentVersion")
			cncl()
			i.AgentVersion = fmt.Sprint(av)
		}
	}
}

type discoverMsg struct {
	peer.ID
	*trackingInfo
}

// peerID -> dht.FindPeer -> network.Connect
func scrapePeers(h host.Host, dht *dht.IpfsDHT, peers map[peer.ID]*trackingInfo, lg *libgeo.GeoIP) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := make(chan discoverMsg, 30)
	pcw := sync.WaitGroup{}
	ic := make(chan discoverMsg, 30)
	icw := sync.WaitGroup{}

	pcw.Add(200)
	icw.Add(100)
	for i := 0; i < 100; i++ {
		go peerFinder(ctx, dht, pc, ic, &pcw)
		go peerFinder(ctx, dht, pc, ic, &pcw)
		go connector(ctx, h, ic, &icw)
		time.Sleep(time.Millisecond * 100)
	}

	i := 0
	for p := range peers {
		i++
		pc <- discoverMsg{p, peers[p]}
		if i%100 == 0 {
			fmt.Printf(".")
		}
	}

	close(pc)
	pcw.Wait()
	close(ic)
	icw.Wait()

	conn := 0
	for _, c := range peers {
		node_counts_g.WithLabelValues(c.AgentVersion, countryOf(lg, c.Addresses)).Inc()
		if c.Connected {
			conn++
			node_connectable_g.WithLabelValues(c.AgentVersion, countryOf(lg, c.Addresses)).Inc()
		}
		if len(c.Addresses) > 0 {
			addr_counts_s.Observe(float64(len(c.Addresses)))
		}
	}
	fmt.Printf("%d of %d nodes connected.\n", conn, len(peers))

	return nil
}

func countryOf(lg *libgeo.GeoIP, addrs []ma.Multiaddr) string {
	country := "ZZ"
	for _, a := range addrs {
		if manet.IsPublicAddr(a) {
			ip, _ := manet.ToIP(a)
			if ip.To4().Equal(ip) {
				loc := lg.GetLocationByIP(ip.String())
				country = loc.CountryCode
			} else {
				country = "V6"
			}
			break
		}
	}

	return country
}
