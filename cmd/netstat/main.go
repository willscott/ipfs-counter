package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/VividCortex/gohistogram"
	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/pflag"
)

// a DB entry
type trackingInfo struct {
	Addresses     []string  `json:"a"`
	LastConnected time.Time `json:"lc"`
	LastSeen      time.Time `json:"ls"`
	FirstSeen     time.Time `json:"fs"`
	Sightings     int       `json:"n"`
	AgentVersion  string    `json:"av"`
	Protocols     []string  `json:"ps"`
}

var window *time.Duration = pflag.DurationP("window", "w", time.Hour*24*7, "window to track over")
var bucket *int = pflag.IntP("buckets", "b", 10, "how many histogram buckets")
var dbName *string = pflag.StringP("database", "d", "netdata", "where the database lives")

// To report:
// what fraction of nodes were connected to?
// distribution of versions
func main() {
	pflag.Parse()

	db, err := badger.Open(badger.DefaultOptions(*dbName))

	if err != nil {
		panic(err)
	}

	versions := make(map[string]int)
	seen := 0
	connectable := 0
	windowStart := time.Now().Add(*window * time.Duration(-1))

	lifetimes := gohistogram.NewHistogram(50)
	sightings := gohistogram.NewHistogram(50)

	putInMap := func(key string) func(val []byte) error {
		return func(val []byte) error {
			row := trackingInfo{}
			err := json.Unmarshal(val, &row)
			if err != nil {
				return err
			}

			if row.LastSeen.Before(windowStart) {
				return nil
			}

			if _, ok := versions[row.AgentVersion]; !ok {
				versions[row.AgentVersion] = 0
			}
			versions[row.AgentVersion]++
			seen++
			if !row.LastConnected.IsZero() {
				connectable++
			}

			lt := row.LastSeen.Sub(row.FirstSeen)
			lifetimes.Add(float64(lt) / float64(time.Second))
			sightings.Add(float64(row.Sightings))

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

	fmt.Printf("%f%% (%d/%d) peers were connectable in the last %s.\n", float64(connectable)/float64(seen)*100.0, connectable, seen, window)
	fmt.Printf("Peer Lifetimes:\n%s\n", PrettyString(lifetimes, true))
	fmt.Printf("Peer sightings:\n%s\n", PrettyString(sightings, false))
	fmt.Printf("Version counts:\n")
	other := 0
	for v, c := range versions {
		if c > 100 {
			fmt.Printf("%-15s %d\n", v, c)
		} else {
			other += c
		}
	}
	fmt.Printf("%-15s %d\n", "other", other)
}

func PrettyString(n *gohistogram.NumericHistogram, isTime bool) (str string) {
	for i := 0; i < 10; i++ {
		q := n.Quantile(float64(i+1) / 10.0)
		if isTime {
			str += fmt.Sprintf("%02d%%\t %s\n", 10*i, time.Duration(q*float64(time.Second)))
		} else {
			str += fmt.Sprintf("%02d%%\t %04f\n", 10*i, q)
		}
	}
	return
}
