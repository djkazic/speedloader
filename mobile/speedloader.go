package lndmobile

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/breez/breez/refcount"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightningnetwork/lnd/channeldb"
	"go.etcd.io/bbolt"
)

const (
	directoryPattern = "data/graph/{{network}}/"
)

var (
	ErrMissingPolicyError = errors.New("missing channel policy")
	serviceRefCounter     refcount.ReferenceCountable
	chanDB                *channeldb.DB
	bucketsToCopy         = map[string]struct{}{
		"graph-edge": {},
		"graph-node": {},
	}
)

func createService(workingDir string) (*channeldb.DB, error) {
	var err error
	graphDir := path.Join(workingDir, strings.Replace(directoryPattern, "{{network}}", "mainnet", -1))
	fmt.Println("creating shared channeldb service.")
	chanDB, err := channeldb.Open(graphDir,
		channeldb.OptionSetSyncFreelist(true))
	if err != nil {
		fmt.Printf("unable to open channeldb: %v\n", err)
		return nil, err
	}

	fmt.Println("channeldb was opened successfuly")
	return chanDB, err
}

func release() error {
	return chanDB.Close()
}

func newService(workingDir string) (db *channeldb.DB, rel refcount.ReleaseFunc, err error) {
	chanDB, err = createService(workingDir)
	if err != nil {
		return nil, nil, err
	}
	return chanDB, release, err
}

func walkBucket(b *bbolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc, skip skipFunc) error {
	if skip != nil && skip(keypath, k, v) {
		return nil
	}
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}
	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}
	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn, skip)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn, skip)
	})
}

func walk(db *bbolt.DB, walkFn walkFunc, skipFn skipFunc) error {
	return db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn, skipFn)
		})
	})
}

// Merge copies from source to dest and ignoring items using the skip function.
// It is different from Compact in that it tries to create a bucket only if not exists.
func merge(tx *bbolt.Tx, src *bbolt.DB, skip skipFunc) error {
	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}
		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}
		// Fill the entire page for best compaction.
		b.FillPercent = 1.0
		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}
		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}, skip); err != nil {
		return err
	}
	return nil
}

type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

type skipFunc func(keys [][]byte, k, v []byte) bool

func ourNode(chanDB *channeldb.DB) (*channeldb.LightningNode, error) {
	graph := chanDB.ChannelGraph()
	node, err := graph.SourceNode()
	if err == channeldb.ErrSourceNodeNotSet || err == channeldb.ErrGraphNotFound {
		return nil, nil
	}
	return node, err
}

func ourData(tx walletdb.ReadWriteTx, ourNode *channeldb.LightningNode) (
	[]*channeldb.LightningNode, []*channeldb.ChannelEdgeInfo, []*channeldb.ChannelEdgePolicy, error) {

	nodeMap := make(map[string]*channeldb.LightningNode)
	var edges []*channeldb.ChannelEdgeInfo
	var policies []*channeldb.ChannelEdgePolicy

	err := ourNode.ForEachChannel(tx, func(tx walletdb.ReadTx,
		channelEdgeInfo *channeldb.ChannelEdgeInfo,
		toPolicy *channeldb.ChannelEdgePolicy,
		fromPolicy *channeldb.ChannelEdgePolicy) error {

		if toPolicy == nil || fromPolicy == nil {
			return nil
		}
		nodeMap[hex.EncodeToString(toPolicy.Node.PubKeyBytes[:])] = toPolicy.Node
		edges = append(edges, channelEdgeInfo)
		if toPolicy != nil {
			policies = append(policies, toPolicy)
		}
		if fromPolicy != nil {
			policies = append(policies, fromPolicy)
		}
		return nil
	})

	if err != nil {
		return nil, nil, nil, err
	}
	var nodes []*channeldb.LightningNode
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}
	return nodes, edges, policies, nil
}

func putOurData(chanDB *channeldb.DB, node *channeldb.LightningNode, nodes []*channeldb.LightningNode,
	edges []*channeldb.ChannelEdgeInfo, policies []*channeldb.ChannelEdgePolicy) error {

	graph := chanDB.ChannelGraph()
	err := graph.SetSourceNode(node)
	if err != nil {
		return fmt.Errorf("graph.SetSourceNode(%x): %w", node.PubKeyBytes, err)
	}
	for _, n := range nodes {
		err = graph.AddLightningNode(n)
		if err != nil {
			return fmt.Errorf("graph.AddLightningNode(%x): %w", n.PubKeyBytes, err)
		}
	}
	for _, edge := range edges {
		err = graph.AddChannelEdge(edge)
		if err != nil && err != channeldb.ErrEdgeAlreadyExist {
			return fmt.Errorf("graph.AddChannelEdge(%x): %w", edge.ChannelID, err)
		}
	}
	for _, policy := range policies {
		err = graph.UpdateEdgePolicy(policy)
		if err != nil {
			return fmt.Errorf("graph.UpdateEdgePolicy(): %w", err)
		}
	}
	return nil
}

func hasSourceNode(tx *bbolt.Tx) bool {
	nodes := tx.Bucket([]byte("graph-node"))
	if nodes == nil {
		return false
	}
	selfPub := nodes.Get([]byte("source"))
	return selfPub != nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func GossipSync(cacheDir string, dataDir string, networkType string, callback Callback) {
	var (
		firstRun      bool
		useDGraph     bool
		dgraphPath    = cacheDir + "/dgraph/channel.db"
		breezURL      = "https://maps.eldamar.icu/mainnet/graph/graph-001d.db"
		checksumURL   = "https://maps.eldamar.icu/mainnet/graph/MD5SUMS"
		checksumValue string
	)
	// check lastRun time, return early if we ran too recently
	lastRunPath := cacheDir + "/lastrun"
	if !fileExists(lastRunPath) {
		os.Create(lastRunPath)
		firstRun = true
	}
	lastRun, err := os.Stat(lastRunPath)
	if err == nil {
		modifiedTime := lastRun.ModTime()
		now := time.Now()
		diff := now.Sub(modifiedTime)
		if !firstRun && diff.Hours() <= 24 {
			// this is not the first run and
			// we have run speedloader within the last 24h, abort
			callback.OnResponse([]byte("skip_time_constraint"))
			return
		}
	}
	// checksum fetching
	client := new(http.Client)
	req, err := http.NewRequest("GET", checksumURL, nil)
	if err != nil {
		callback.OnError(err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		callback.OnError(err)
		return
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				callback.OnError(err)
				return
			}
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			callback.OnError(errors.New("unexpected_checksum_line_fmt"))
			return
		}
		filename := fields[1]
		hash := fields[0]
		if filename == "graph-001d.db" {
			checksumValue = hash
		}
	}
	if checksumValue != "" {
		// we have a valid checksum
		fh, openErr := os.Open(dgraphPath)
		// do we have a file?
		if !os.IsNotExist(openErr) {
			// first, calculate the md5sum of the file we have
			defer fh.Close()
			md5h := md5.New()
			_, err = io.Copy(md5h, fh)
			if err != nil {
				callback.OnError(err)
				return
			}
			sum := md5h.Sum(nil)
			calculatedChecksum := hex.EncodeToString(sum)
			if checksumValue != calculatedChecksum {
				// failed checksum check (existing file)
				// unconditionally try to delete dgraph file and lastRun
				os.Remove(dgraphPath)
				os.Remove(lastRunPath)
			} else {
				// checksum matches
				// now check modtime
				info, err := os.Stat(dgraphPath)
				// check the modified time on the existing downloaded channel.db, see if it is <= 48h old
				if err == nil {
					modifiedTime := info.ModTime()
					now := time.Now()
					diff := now.Sub(modifiedTime)
					if diff.Hours() <= 48 {
						// abort downloading the graph, we have a fresh-enough downloaded graph
						useDGraph = true
					}
				}
			}
		}
	}
	if networkType != "wifi" && networkType != "ethernet" {
		useDGraph = true
	}
	// if the dgraph is not usable
	if !useDGraph {
		// download the breez gossip database
		os.MkdirAll(cacheDir+"/dgraph", 0777)
		out, err := os.Create(dgraphPath)
		if err != nil {
			callback.OnError(err)
			return
		}
		client := new(http.Client)
		req, err := http.NewRequest("GET", breezURL, nil)
		if err != nil {
			callback.OnError(err)
			return
		}
		req.Header.Add("Accept-Encoding", "br, gzip")
		resp, err := client.Do(req)
		if err != nil {
			callback.OnError(err)
			return
		}
		var reader io.Reader
		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			reader, err = gzip.NewReader(resp.Body)
			if err != nil {
				callback.OnError(err)
				return
			}
		case "br":
			reader = brotli.NewReader(resp.Body)
		default:
			reader = resp.Body
		}
		_, err = io.Copy(out, reader)
		if err != nil {
			callback.OnError(err)
			return
		}
		out.Close()
		resp.Body.Close()
		fh, err := os.Open(dgraphPath)
		if err != nil {
			callback.OnError(err)
			return
		}
		defer fh.Close()
		md5h := md5.New()
		_, err = io.Copy(md5h, fh)
		if err != nil {
			callback.OnError(err)
			return
		}
		sum := md5h.Sum(nil)
		calculatedChecksum := hex.EncodeToString(sum)
		if checksumValue != calculatedChecksum {
			// failed checksum check (just downloaded file)
			// unconditionally remove dgraph file and lastRun
			os.Remove(dgraphPath)
			os.Remove(lastRunPath)
			callback.OnResponse([]byte("skip_checksum_failed"))
			return
		}
	}
	// open channel.db as dest
	service, release, err := serviceRefCounter.Get(
		func() (interface{}, refcount.ReleaseFunc, error) {
			return newService(dataDir)
		},
	)
	if err != nil {
		callback.OnError(err)
		return
	}
	defer release()
	destDB := service.(*channeldb.DB)
	// open dgraph.db as source
	dchanDB, err := channeldb.Open(cacheDir + "/dgraph")
	if err != nil {
		callback.OnError(err)
		return
	}
	defer dchanDB.Close()
	sourceDB, err := bdb.UnderlineDB(dchanDB.Backend)
	if err != nil {
		callback.OnError(err)
		return
	}
	// utility function to convert bolts key to a string path.
	extractPathElements := func(bytesPath [][]byte, key []byte) []string {
		var path []string
		for _, b := range bytesPath {
			path = append(path, string(b))
		}
		return append(path, string(key))
	}
	ourNode, err := ourNode(destDB)
	if err != nil {
		callback.OnError(err)
		return
	}
	kvdbTx, err := destDB.BeginReadWriteTx()
	if err != nil {
		callback.OnError(err)
		return
	}
	tx, err := bdb.UnderlineTX(kvdbTx)
	if err != nil {
		callback.OnError(err)
		return
	}
	defer tx.Rollback()
	if ourNode == nil && hasSourceNode(tx) {
		callback.OnError(err)
		return
		//errors.New("source node was set before sync transaction, rolling back").Error()
	}
	if ourNode != nil {
		channelNodes, channels, policies, err := ourData(kvdbTx, ourNode)
		if err != nil {
			callback.OnError(err)
			return
		}

		// add our data to the source db.
		if err := putOurData(dchanDB, ourNode, channelNodes, channels, policies); err != nil {
			callback.OnError(err)
			return
		}
	}
	// clear graph data from the destination db
	for b := range bucketsToCopy {
		if err := tx.DeleteBucket([]byte(b)); err != nil && err != bbolt.ErrBucketNotFound {
			callback.OnError(err)
			return
		}
	}
	err = merge(tx, sourceDB,
		func(keyPath [][]byte, k []byte, v []byte) bool {
			pathElements := extractPathElements(keyPath, k)
			_, shouldCopy := bucketsToCopy[pathElements[0]]
			return !shouldCopy
		})
	if err != nil {
		callback.OnError(err)
		return
	}
	// update the lastrun modified time
	now := time.Now()
	err = os.Chtimes(lastRunPath, now, now)
	if err != nil {
		callback.OnError(err)
		return
	}
	callback.OnResponse([]byte(fmt.Sprintf("dl=%t,done_commit_err=%v", !useDGraph, tx.Commit())))
}
