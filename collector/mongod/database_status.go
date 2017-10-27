package collector_mongod

import (
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
	}, []string{"db"})
	dataSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "data_size_bytes",
		Help:      "The total size in bytes of the uncompressed data held in this database",
	}, []string{"db"})
	collectionsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "collections_total",
		Help:      "Contains a count of the number of collections in that database",
	}, []string{"db"})
	indexesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "indexes_total",
		Help:      "Contains a count of the total number of indexes across all collections in the database",
	}, []string{"db"})
	objectsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db",
		Name:      "objects_total",
		Help:      "Contains a count of the number of objects (i.e. documents) in the database across all collections",
	}, []string{"db"})
)

// DatabaseStatus represents stats about a database (mongod and raw from mongos)
type DatabaseStatus struct {
	Name        string `bson:"db,omitempty"`
	IndexSize   int    `bson:"indexSize,omitempty"`
	DataSize    int    `bson:"dataSize,omitempty"`
	Collections int    `bson:"collections,omitempty"`
	Objects     int    `bson:"objects,omitempty"`
	Indexes     int    `bson:"indexes,omitempty"`
}

// Export exports database stats to prometheus
func (dbStatus *DatabaseStatus) Export(ch chan<- prometheus.Metric) {
	indexSize.WithLabelValues(dbStatus.Name).Set(float64(dbStatus.IndexSize))
	dataSize.WithLabelValues(dbStatus.Name).Set(float64(dbStatus.DataSize))
	collectionsTotal.WithLabelValues(dbStatus.Name).Set(float64(dbStatus.Collections))
	indexesTotal.WithLabelValues(dbStatus.Name).Set(float64(dbStatus.Indexes))
	objectsTotal.WithLabelValues(dbStatus.Name).Set(float64(dbStatus.Objects))

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
		if db != "admin" && db != "test" && db != "local" {
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
