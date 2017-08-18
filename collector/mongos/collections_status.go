package collector_mongos

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	collectionSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "size",
		Help:      "The total size in memory of all records in a collection",
	}, []string{"db", "coll"})
	collectionObjectCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "count",
		Help:      "The number of objects or documents in this collection",
	}, []string{"db", "coll"})
	collectionAvgObjSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "avgobjsize",
		Help:      "The average size of an object in the collection (plus any padding)",
	}, []string{"db", "coll"})
	collectionStorageSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "storage_size",
		Help:      "The total amount of storage allocated to this collection for document storage",
	}, []string{"db", "coll"})
	collectionIndexes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes",
		Help:      "The number of indexes on the collection",
	}, []string{"db", "coll"})
	collectionIndexesSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes_size",
		Help:      "The total size of all indexes",
	}, []string{"db", "coll"})
)

// DatabaseStatus represents stats about a database (mongod and raw from mongos)

type CollectionStatus struct {
	Size        int `bson:"size,omitempty"`
	Count       int `bson:"count,omitempty"`
	AvgObjSize  int `bson:"avgObjSize,omitempty"`
	StorageSize int `bson:"storageSize,omitempty"`
	Indexes     int `bson:"indexSizes,omitempty"`
	IndexesSize int `bson:"totalIndexSize,omitempty"`
}

// Export exports database stats to prometheus
func (collStatus *CollectionStatus) Export(ch chan<- prometheus.Metric, db string, coll string) {
	collectionSize.WithLabelValues(db, coll).Set(float64(collStatus.Size))
	collectionObjectCount.WithLabelValues(db, coll).Set(float64(collStatus.Count))
	collectionAvgObjSize.WithLabelValues(db, coll).Set(float64(collStatus.AvgObjSize))
	collectionStorageSize.WithLabelValues(db, coll).Set(float64(collStatus.StorageSize))
	collectionIndexes.WithLabelValues(db, coll).Set(float64(collStatus.Indexes))

	collectionSize.Collect(ch)
	collectionObjectCount.Collect(ch)
	collectionAvgObjSize.Collect(ch)
	collectionStorageSize.Collect(ch)
	collectionIndexes.Collect(ch)

	collectionSize.Reset()
	collectionObjectCount.Reset()
	collectionAvgObjSize.Reset()
	collectionStorageSize.Reset()
	collectionIndexes.Reset()
}

// Describe describes database stats for prometheus
func (collStatus *CollectionStatus) Describe(ch chan<- *prometheus.Desc) {
	collectionSize.Describe(ch)
	collectionObjectCount.Describe(ch)
	collectionAvgObjSize.Describe(ch)
	collectionStorageSize.Describe(ch)
	collectionIndexes.Describe(ch)
	collectionIndexesSize.Describe(ch)
}

// GetDatabaseStatus returns stats for a given database
func GetCollectionStatus(session *mgo.Session, db string, collection string) *CollectionStatus {
	collStatus := &CollectionStatus{}
	err := session.DB(db).Run(bson.D{{"collStats", collection}, {"scale", 1}}, &collStatus)
	if err != nil {
		log.Error("Failed to get collection status.")
		return nil
	}

	return collStatus
}
