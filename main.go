package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	ds "github.com/ipfs/go-datastore"
	ipfsaddr "github.com/ipfs/go-ipfs-addr"
	"github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

const CRAWL_PERIOD = time.Second * 15

var (
	node_counts_g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "nodes_total",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "total number of nodes seen in a given time period",
	}, []string{"interval", "version"})

	protocols_g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "protocols",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "protocol counts by name",
	}, []string{"interval", "protocol"})

	query_lat_h = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "query",
		Subsystem: "dht",
		Namespace: "libp2p",
		Help:      "dht 'findclosestpeers' latencies",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1, 2, 5, 10, 15, 20, 25, 30, 60},
	})

	addr_counts_s = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:      "addr_counts",
		Subsystem: "stats",
		Namespace: "libp2p",
		Help:      "address counts discovered by the dht crawls",
	})

	query_lifetime_h = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "lifetime",
		Subsystem: "dht",
		Namespace: "libp2p",
		Help:      "how long dht servers remain online",
		Buckets:   []float64{1, 5, 15, 60, 60 * 5, 60 * 15, 3600, 3600 * 2, 3600 * 4, 3600 * 12, 3600 * 24, 3600 * 48, 3600 * 72, 3600 * 24 * 7, 3600 * 24 * 14, 3600 * 24 * 30},
	})

	query_offtime_h = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:      "offtime",
		Subsystem: "dht",
		Namespace: "libp2p",
		Help:      "how long servers stay offline before coming back",
		Buckets:   []float64{1, 5, 15, 60, 60 * 5, 60 * 15, 3600, 3600 * 2, 3600 * 4, 3600 * 12, 3600 * 24, 3600 * 48, 3600 * 72, 3600 * 24 * 7, 3600 * 24 * 14, 3600 * 24 * 30},
	})
)

func init() {
	prometheus.MustRegister(node_counts_g)
	prometheus.MustRegister(protocols_g)
	prometheus.MustRegister(query_lat_h)
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

func handlePromWithAuth(w http.ResponseWriter, r *http.Request) {
	u, p, ok := r.BasicAuth()
	if !ok {
		w.WriteHeader(403)
		return
	}

	if !(u == "protocol" && p == os.Getenv("IPFS_METRICS_PASSWORD")) {
		w.WriteHeader(403)
		return
	}

	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	http.HandleFunc("/metrics", handlePromWithAuth)
	go func() {
		if err := http.ListenAndServe(":1234", nil); err != nil {
			panic(err)
		}
	}()

	for {
		if err := buildHostAndScrapePeers(); err != nil {
			log.Error("scrape failed: ", err)
		}
	}
}

func buildHostAndScrapePeers() error {
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

	mds := ds.NewMapDatastore()
	mdht, err := dht.New(ctx, h,
		dht.Datastore(mds),
		dht.Mode(dht.ModeClient),
		dht.Validator(record.NamespacedValidator{
			"pk":   record.PublicKeyValidator{},
			"ipns": ipns.Validator{KeyBook: h.Peerstore()}}))
	if err != nil {
		panic(err)
	}

	bootstrap(ctx, h)

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
			case le := <-evch:
				kpl.Lock()
				for _, heardPeer := range le.Response.Heard {
					seen++
					t, ok := knownPeers[heardPeer.Peer]
					if !ok {
						newSeen++
						t = new(trackingInfo)
						knownPeers[heardPeer.Peer] = t
					}
					t.LastSeen = time.Now()
				}
				kpl.Unlock()
			}
		}
	}()

	prevSeen := 0
	prevNew := 0
	for {
		pokeDHT(ctx, mdht)
		kpl.Lock()
		// go until a process of poking 1000 peers results in finding less than 50 new ones.
		if seen-prevSeen > 1000 {
			fmt.Printf("new rate is %f\n", float64(newSeen-prevNew)/float64(seen-prevSeen))
			if float64(newSeen-prevNew)/float64(seen-prevSeen) < 0.05 {
				kpl.Unlock()
				break
			}
			prevSeen = seen
			prevNew = newSeen
		}
		kpl.Unlock()
		time.Sleep(CRAWL_PERIOD)
	}
	fmt.Printf("through the DHT, learned about %d peers\n", seen)
	if err := scrapePeers(h, mdht, knownPeers); err != nil {
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

type trackingInfo struct {
	Addresses     []string  `json:"a"`
	LastConnected time.Time `json:"lc"`
	LastSeen      time.Time `json:"ls"`
	FirstSeen     time.Time `json:"fs"`
	Sightings     int       `json:"n"`
	AgentVersion  string    `json:"av"`
	Protocols     []string  `json:"ps"`
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
		log.Error(err)
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

func peerFinder(ctx context.Context, dht *dht.IpfsDHT, ids chan peer.ID, infos chan peer.AddrInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-ids:
			if !ok {
				return
			}
			info, err := dht.FindPeer(ctx, p)
			if err != nil {
				infos <- info
			}
		}
	}
}

func connector(ctx context.Context, h host.Host, infos chan peer.AddrInfo, versions chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case i, ok := <-infos:
			if !ok {
				return
			}
			err := h.Connect(ctx, i)
			if err != nil {
				continue
			}
			if h.Network().Connectedness(i.ID) == network.Connected {
				av, _ := h.Peerstore().Get(i.ID, "AgentVersion")
				versions <- fmt.Sprint(av)
			}
		}
	}
}

// peerID -> dht.FindPeer -> network.Connect
func scrapePeers(h host.Host, dht *dht.IpfsDHT, peers map[peer.ID]*trackingInfo) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := make(chan peer.ID, 30)
	pcw := sync.WaitGroup{}
	ic := make(chan peer.AddrInfo, 30)
	icw := sync.WaitGroup{}
	vc := make(chan string, 30)
	pcw.Add(15)
	icw.Add(15)
	for i := 0; i < 15; i++ {
		go peerFinder(ctx, dht, pc, ic, &pcw)
		go connector(ctx, h, ic, vc, &icw)
	}

	vmap := make(map[string]int)
	go func() {
		for {
			select {
			case v, ok := <-vc:
				if !ok {
					return
				}
				vmap[v]++
			}
		}
	}()

	pl := len(peers)
	for p := range peers {
		pc <- p
	}

	close(pc)
	pcw.Wait()
	close(ic)
	icw.Wait()
	close(vc)

	cl := 0
	for v, c := range vmap {
		cl += c
		if c > 100 {
			fmt.Printf("%-15s\t%d\n", v, c)
		}
	}
	fmt.Printf("%d of %d nodes connected.\n", pl, cl)

	return nil
}
