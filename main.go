package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syndtr/goleveldb/leveldb"

	badger "github.com/dgraph-io/badger/v2"
	ds "github.com/ipfs/go-datastore"
	ipfsaddr "github.com/ipfs/go-ipfs-addr"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

const CRAWL_PERIOD = time.Second * 60

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
	"/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",  // mars.i.ipfs.io
	"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM", // pluto.i.ipfs.io
	"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu", // saturn.i.ipfs.io
	"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",   // venus.i.ipfs.io
	"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",  // earth.i.ipfs.io
	"/ip4/104.236.151.122/tcp/4001/ipfs/QmSoLju6m7xTh3DuokvT3886QRYqxAzb1kShaanJgW36yx",
	"/ip4/188.40.114.11/tcp/4001/ipfs/QmZY7MtK8ZbG1suwrxc7xEYZ2hQLf1dAWPRHhjxC8rjq8E",
	"/ip4/5.9.59.34/tcp/4001/ipfs/QmRv1GNseNP1krEwHDjaQMeQVJy41879QcDwpJVhY8SWve",
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

	db, err := badger.Open(badger.DefaultOptions("netdata"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var churnDB *badger.DB

	// TODO: add in a real args parser once complexity warrants it.
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--no-churn" {
		fmt.Printf("Skipping Churn counts")
	} else {
		churnDB, err = badger.Open(badger.DefaultOptions("churndata"))
		if err != nil {
			panic(err)
		}
		defer churnDB.Close()
	}

	if err := getStats(db); err != nil {
		log.Error("get stats failed: ", err)
	}

	for {
		if err := buildHostAndScrapePeers(db, churnDB); err != nil {
			log.Error("scrape failed: ", err)
		}
	}
}

func buildHostAndScrapePeers(db *badger.DB, churnDB *badger.DB) error {
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
	mdht := dht.NewDHT(ctx, h, mds)

	bootstrap(ctx, h)

	for {
		if err := scrapePeers(db, churnDB, h, mdht); err != nil {
			return err
		}
		time.Sleep(CRAWL_PERIOD)
	}
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

type churnAddr []churnInfo

type churnInfo struct {
	PeerID       string        `json:"p"`
	FirstSeen    time.Time     `json:"fs"`
	Lifetime     time.Duration `json:"d"`
	failedScrape bool          `json:"f"`
}

func getStats(db *badger.DB) error {
	now := time.Now()

	protosCountDay := make(map[string]int)
	protosCountHour := make(map[string]int)
	dayVersCount := make(map[string]int)
	hourVersCount := make(map[string]int)
	var dayPeerCount int
	var hourPeerCount int

	iterTx := db.NewTransaction(false)
	defer iterTx.Discard()
	iter := iterTx.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		pid := peer.ID(item.Key())
		val, _ := item.ValueCopy(nil)

		var ti trackingInfo
		err := json.Unmarshal(val, &ti)
		if err != nil {
			log.Error("invalid json in leveldb for peer: ", pid.Pretty())
			continue
		}

		age := now.Sub(ti.LastSeen)
		if age <= time.Hour*24 {
			dayVersCount[ti.AgentVersion]++
			dayPeerCount++

			for _, p := range ti.Protocols {
				protosCountDay[p]++
			}
		}
		if age <= time.Hour {
			hourVersCount[ti.AgentVersion]++
			hourPeerCount++
			for _, p := range ti.Protocols {
				protosCountHour[p]++
			}
		}

	}

	for k, v := range dayVersCount {
		node_counts_g.WithLabelValues("day", k).Set(float64(v))
	}
	for k, v := range hourVersCount {
		node_counts_g.WithLabelValues("hour", k).Set(float64(v))
	}
	for k, v := range protosCountDay {
		protocols_g.WithLabelValues("day", k).Set(float64(v))
	}
	for k, v := range protosCountHour {
		protocols_g.WithLabelValues("hour", k).Set(float64(v))
	}

	return nil
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

func scrapePeers(db *badger.DB, churnDB *badger.DB, h host.Host, mdht *dht.IpfsDHT) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	rlim := make(chan struct{}, 10)
	fmt.Printf("scraping")
	scrapeRound := func(k string) {
		mctx, cancel := context.WithTimeout(ctx, time.Second*30)
		defer cancel()
		defer wg.Done()
		defer fmt.Print(".")
		rlim <- struct{}{}
		defer func() {
			<-rlim
		}()

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

	for i := 0; i < 15; i++ {
		wg.Add(1)
		go scrapeRound(getRandomString())
	}
	wg.Wait()
	fmt.Println("done!")

	peers := h.Peerstore().Peers()
	conns := h.Network().Conns()

	connected := make(map[peer.ID]bool)
	for _, c := range conns {
		connected[c.RemotePeer()] = true
	}

	var churnTX *badger.Txn
	if churnDB != nil {
		churnTX = churnDB.NewTransaction(true)
	}

	tx := db.NewTransaction(true)

	now := time.Now()
	var pstat *trackingInfo
	for _, p := range peers {
		if p == h.ID() {
			continue
		}
		val, err := tx.Get([]byte(p))
		switch err {
		case leveldb.ErrNotFound:
			pstat = &trackingInfo{
				FirstSeen: now,
			}
		default:
			log.Error("getting data from leveldb: ", err)
			continue
		case nil:
			valdat, _ := val.ValueCopy(nil)

			pstat = new(trackingInfo)
			if err = json.Unmarshal(valdat, pstat); err != nil {
				log.Error("leveldb had bad json data: ", err)
			}
		}

		pstat.Sightings++
		if connected[p] {
			pstat.LastConnected = now
		}
		pstat.LastSeen = now

		cutoff := time.Now().Add(-3 * CRAWL_PERIOD)
		addrs := h.Peerstore().Addrs(p)
		prevAddrs := pstat.Addresses
		pstat.Addresses = nil // reset
		for _, a := range addrs {
			var addrChurnStat churnAddr
			cur, err := churnTX.Get([]byte(a.String()))
			if err == nil {
				curDat, _ := cur.ValueCopy(nil)
				err = json.Unmarshal(curDat, addrChurnStat)
			}

			found := false
			if has(a.String(), prevAddrs) {
				// see if there's a non-dead entry to extend.
				for _, ac := range addrChurnStat {
					if ac.PeerID == p.String() && !ac.failedScrape {
						ac.Lifetime = time.Since(ac.FirstSeen)
						found = true
						break
					}
				}
			}

			if !found {
				// kill entries where:
				// a. it's this peer, where we saw them at a point where they weren't advertising this IP
				// b. the lifetime of ost recent tme seen is long enough ago that we should have crawled / foudn this to update it.
				for _, ac := range addrChurnStat {
					if !ac.failedScrape && (ac.FirstSeen.Add(ac.Lifetime).Before(cutoff) ||
						ac.PeerID == p.String()) {
						ac.failedScrape = true
					}
				}
				addrChurnStat = append(addrChurnStat, churnInfo{
					PeerID:       p.String(),
					FirstSeen:    time.Now(),
					Lifetime:     0,
					failedScrape: false,
				})
			}
			data, err := json.Marshal(addrChurnStat)
			if err != nil {
				log.Error("failed to json marshal addrChurnStat: ", err)
				continue
			}
			if err := churnTX.Set([]byte(a.String()), data); err != nil {
				log.Error("failed to write to leveldb: ", err)
				continue
			}
			pstat.Addresses = append(pstat.Addresses, a.String())
		}
		av, err := h.Peerstore().Get(p, "AgentVersion")
		if err == nil {
			pstat.AgentVersion = fmt.Sprint(av)
		}
		protos, _ := h.Peerstore().GetProtocols(p)
		if len(protos) != 0 {
			pstat.Protocols = protos
		}

		data, err := json.Marshal(pstat)
		if err != nil {
			log.Error("failed to json marshal pstat: ", err)
			continue
		}
		if err := tx.Set([]byte(p), data); err != nil {
			log.Error("failed to write to leveldb: ", err)
			continue
		}
	}
	if err := tx.Commit(); err != nil {
		log.Error("failed to commit update transaction: ", err)
	}

	if churnTX != nil {
		if err := churnTX.Commit(); err != nil {
			log.Error("failed to commit update transaction: ", err)
		}
	}

	fmt.Printf("updating stats took %s\n", time.Since(now))

	if err := getStats(db); err != nil {
		return err
	}

	return nil
}
