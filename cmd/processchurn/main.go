package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/pflag"
)

// a DB entry
type churnInfo struct {
	PeerID       string        `json:"p"`
	FirstSeen    time.Time     `json:"fs"`
	Lifetime     time.Duration `json:"d"`
	failedScrape bool          `json:"f"`
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

	putInMap := func(val []byte) error {
		churn := new(churnInfo)
		err := json.Unmarshal(val, churn)
		if err != nil {
			return err
		}
		if churn.FirstSeen.Add(churn.Lifetime).Before(windowStart) {
			return nil
		}
		if _, ok := mapByPeer[churn.PeerID]; !ok {
			mapByPeer[churn.PeerID] = make([]*churnInfo, 1)
		}
		mapByPeer[churn.PeerID] = append(mapByPeer[churn.PeerID], churn)
		return nil
	}

	// pull into memory
	tx := db.NewTransaction(false)
	defer tx.Discard()
	iter := tx.NewIterator(badger.DefaultIteratorOptions)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		itm := iter.Item()
		itm.Value(putInMap)
	}
	iter.Close()

	fmt.Printf("Saw %d peers.\n", len(mapByPeer))
}
