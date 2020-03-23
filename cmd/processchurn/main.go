package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/pflag"
)

// a DB entry
type churnInfo struct {
	PeerID    string        `json:"p"`
	FirstSeen time.Time     `json:"fs"`
	Lifetime  time.Duration `json:"d"`
	address   string
}

var window *time.Duration = pflag.DurationP("window", "w", time.Hour*24*7, "window to track over")
var bucket *int = pflag.IntP("buckets", "b", 10, "how many histogram buckets")
var dbName *string = pflag.StringP("database", "d", "churndata", "where the churn database lives")

// To report:
// how many distinct peers were seen in the period?
// for a session, how long was it? (the distribution)
// as a session ends, how likely is it that the peer comes back?
// as a session ends, how long will it be until the peer comes back?
func main() {
	pflag.Parse()

	db, err := badger.Open(badger.DefaultOptions(*dbName))

	if err != nil {
		panic(err)
	}

	mapByPeer := make(map[string][]*churnInfo)
	windowStart := time.Now().Add(*window * time.Duration(-1))

	putInMap := func(key string) func(val []byte) error {
		return func(val []byte) error {
			churn := make([]churnInfo, 0)
			err := json.Unmarshal(val, &churn)
			if err != nil {
				return err
			}

			for _, peer := range churn {
				if peer.FirstSeen.Add(peer.Lifetime).Before(windowStart) {
					return nil
				}
				if _, ok := mapByPeer[peer.PeerID]; !ok {
					mapByPeer[peer.PeerID] = make([]*churnInfo, 1)
				}
				peer.address = key
				mapByPeer[peer.PeerID] = append(mapByPeer[peer.PeerID], &peer)
			}
			return nil
		}
	}

	// pull into memory
	tx := db.NewTransaction(false)
	defer tx.Discard()
	iter := tx.NewIterator(badger.DefaultIteratorOptions)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		itm := iter.Item()
		k := string(itm.Key())
		itm.Value(putInMap(k))
	}
	iter.Close()

	fmt.Printf("Saw %d peers.\n", len(mapByPeer))
}
