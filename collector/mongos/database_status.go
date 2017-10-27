package collector_mongos

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	indexSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "index_size_bytes",
		Help:      "The total size in bytes of all indexes created on this database",
	}, []string{"db", "shard"})
	dataSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "data_size_bytes",
		Help:      "The total size in bytes of the uncompressed data held in this database",
	}, []string{"db", "shard"})
	collectionsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "collections_total",
		Help:      "Contains a count of the number of collections in that database",
	}, []string{"db", "shard"})
	indexesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "indexes_total",
		Help:      "Contains a count of the total number of indexes across all collections in the database",
	}, []string{"db", "shard"})
	objectsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "objects_total",
		Help:      "Contains a count of the number of objects (i.e. documents) in the database across all collections",
	}, []string{"db", "shard"})
)

// DatabaseStatus represents stats about a database (mongod and raw from mongos)
type DatabaseStatus struct {
	RawStatus                       // embed to collect top-level attributes
	Shards    map[string]*RawStatus `bson:"raw,omitempty"`
}

// RawStatus represents stats about a database from Mongos side
type RawStatus struct {
	Name        string `bson:"db,omitempty"`
	IndexSize   int    `bson:"indexSize,omitempty"`
	DataSize    int    `bson:"dataSize,omitempty"`
	Collections int    `bson:"collections,omitempty"`
	Objects     int    `bson:"objects,omitempty"`
	Indexes     int    `bson:"indexes,omitempty"`
}

// Export exports database stats to prometheus
func (dbStatus *DatabaseStatus) Export(ch chan<- prometheus.Metric) {
	if len(dbStatus.Shards) > 0 {
		for shard, stats := range dbStatus.Shards {
			shard = strings.Split(shard, "/")[0]
			indexSize.WithLabelValues(stats.Name, shard).Set(float64(stats.IndexSize))
			dataSize.WithLabelValues(stats.Name, shard).Set(float64(stats.DataSize))
			collectionsTotal.WithLabelValues(stats.Name, shard).Set(float64(stats.Collections))
			indexesTotal.WithLabelValues(stats.Name, shard).Set(float64(stats.Indexes))
			objectsTotal.WithLabelValues(stats.Name, shard).Set(float64(stats.Objects))
		}
	}

	indexSize.Collect(ch)
	dataSize.Collect(ch)
	collectionsTotal.Collect(ch)
	indexesTotal.Collect(ch)
	objectsTotal.Collect(ch)

	indexSize.Reset()
	dataSize.Reset()
	collectionsTotal.Reset()
	indexesTotal.Reset()
	objectsTotal.Reset()
}

// Describe describes database stats for prometheus
func (dbStatus *DatabaseStatus) Describe(ch chan<- *prometheus.Desc) {
	indexSize.Describe(ch)
	dataSize.Describe(ch)
	collectionsTotal.Describe(ch)
	indexesTotal.Describe(ch)
	objectsTotal.Describe(ch)
}

// GetDatabaseStatus returns stats for a given database
func GetDatabaseStatus(session *mgo.Session, db string) *DatabaseStatus {
	dbStatus := &DatabaseStatus{}
	err := session.DB(db).Run(bson.D{{"dbStats", 1}, {"scale", 1}}, &dbStatus)
	if err != nil {
		log.Error("Failed to get database status.")
		return nil
	}

	return dbStatus
}

// Collect stats for each database but admin and test
func CollectDatabaseStatus(session *mgo.Session, ch chan<- prometheus.Metric) {
	all, err := session.DatabaseNames()
	if err != nil {
		log.Error("Failed to get database names")
		return
	}

	for _, db := range all {
		if db != "admin" && db != "test" {
			dbStatus := GetDatabaseStatus(session, db)

			if dbStatus != nil {
				dbStatus.Export(ch)
			}

			collAll, err := session.DB(db).CollectionNames()
			if err != nil {
				log.Error("Failed to get collection names")
				return
			}

			for _, coll := range collAll {
				collStatus := GetCollectionStatus(session, db, coll)
				if collStatus != nil {
					collStatus.Export(ch, db, coll)
				}
			}
		}
	}
}
