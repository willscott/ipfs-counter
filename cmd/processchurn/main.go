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
		churn := make(map[string]interface{})
		err := json.Unmarshal(val, &churn)
		if err != nil {
			return err
		}
		lt := time.Duration(0)
		fs := time.Time{}
		pid := ""
		if lx, ok := churn["d"]; ok {
			lt = lx.(time.Duration)
		} else {
			return nil
		}
		if fx, ok := churn["fs"]; ok {
			fs = fx.(time.Time)
		} else {
			return nil
		}
		if px, ok := churn["p"]; ok {
			pid = px.(string)
		} else {
			return nil
		}
		if fs.Add(lt).Before(windowStart) {
			return nil
		}
		if _, ok := mapByPeer[pid]; !ok {
			mapByPeer[pid] = make([]*churnInfo, 1)
		}
		mapByPeer[pid] = append(mapByPeer[pid], &churnInfo{PeerID: pid, FirstSeen: fs, Lifetime: lt})
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
