package mongodb

import (
	"github.com/golang/glog"
	. "github.com/poying/flux/cqrs"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type mongoAggregateRecord struct {
	Id      string  `bson:"_id"`
	Version int     `bson:"version"`
	Events  []Event `bson:"events"`
}

type MongoEventStoreOptions struct {
	Session               *mgo.Session
	Database              string
	EventCollection       string
	AggregateCollection   string
	TransactionCollection string
}

func DefaultMongoEventStoreOptions() *MongoEventStoreOptions {
	return &MongoEventStoreOptions{
		Database:              "",
		EventCollection:       "event",
		AggregateCollection:   "aggregate",
		TransactionCollection: "transaction",
	}
}

// MongoEventStore implementation of the event store
type MongoEventStore struct {
	options *MongoEventStoreOptions
}

func (store *MongoEventStore) getEventCollection() *mgo.Collection {
	return store.options.Session.DB(store.options.Database).C(store.options.EventCollection)
}

func (store *MongoEventStore) getTransactionCollection() *mgo.Collection {
	return store.options.Session.DB(store.options.Database).C(store.options.TransactionCollection)
}

func (store *MongoEventStore) getAggregateCollection() *mgo.Collection {
	return store.options.Session.DB(store.options.Database).C(store.options.AggregateCollection)
}

func (store *MongoEventStore) GetEvents(aggregateId string) []Event {
	collection := store.getAggregateCollection()
	record := &mongoAggregateRecord{}
	err := collection.FindId(aggregateId).One(record)
	if err != nil {
		if err != mgo.ErrNotFound {
			glog.Fatal("error while getting events ", err)
		}
		return []Event{}
	}
	return record.Events
}

func (store *MongoEventStore) GetEventMetaDataFrom(offset, count int) []EventMetaData {
	collection := store.getEventCollection()
	iter := collection.Find(nil).Skip(offset).Limit(count).Iter()
	record := &EventMetaData{}
	metaList := make([]EventMetaData, 0)
	for iter.Next(record) {
		metaList = append(metaList, *record)
	}
	iter.Close()
	return metaList
}

func (store *MongoEventStore) createAggregate(aggregateId string) (bool, error) {
	aggregateCollection := store.getAggregateCollection()
	pipe := aggregateCollection.Pipe([]bson.M{
		bson.M{"$match": bson.M{"_id": aggregateId}},
		bson.M{"$project": bson.M{"version": 1}},
	})

	aggregateMetadata := make(map[string]interface{})
	err := pipe.One(aggregateMetadata)

	if err == mgo.ErrNotFound {
		err = aggregateCollection.Insert(mongoAggregateRecord{
			Id:      aggregateId,
			Version: 0,
		})
		return true, err
	}

	return false, err
}

func (store *MongoEventStore) SaveEvents(aggregateId string, events []Event) error {
	l := len(events)
	if l == 0 {
		return nil
	}

	assert := bson.M{"version": bson.M{"$eq": events[0].AggregateVersion - 1}}

	created, err := store.createAggregate(aggregateId)

	if err != nil {
		return err
	}

	if created {
		assert = nil
	}

	runner := txn.NewRunner(store.getTransactionCollection())
	ops := make([]txn.Op, l+2)

	for index, event := range events {
		ops[index+2] = txn.Op{
			C:      store.options.EventCollection,
			Id:     event.Id,
			Insert: event.EventMetaData,
		}
	}

	ops[0] = txn.Op{
		C:      store.options.AggregateCollection,
		Id:     aggregateId,
		Assert: assert,
		Update: bson.M{
			"$push": bson.M{
				"events": bson.M{
					"$each": events,
				},
			},
		},
	}

	ops[1] = txn.Op{
		C:  store.options.AggregateCollection,
		Id: aggregateId,
		Update: bson.M{
			"$set": bson.M{
				"version": events[l-1].AggregateVersion,
			},
		},
	}

	id := bson.NewObjectId()
	return runner.Run(ops, id, nil)
}

func (store *MongoEventStore) GetEvent(id string) Event {
	eventCollection := store.getEventCollection()
	meta := &EventMetaData{}
	err := eventCollection.FindId(id).One(meta)
	if err != nil {
		if err != mgo.ErrNotFound {
			glog.Fatal("Error while finding event ", err)
		}
		return Event{}
	}
	aggregateCollection := store.getAggregateCollection()
	pipe := aggregateCollection.Pipe([]bson.M{
		bson.M{"$match": bson.M{"_id": meta.AggregateId}},
		bson.M{"$unwind": "$events"},
		bson.M{"$match": bson.M{"events.event_metadata._id": id}},
		bson.M{"$project": bson.M{"events": []string{"$events"}}},
	})
	aggregateRecord := mongoAggregateRecord{}
	err = pipe.One(&aggregateRecord)
	if err != nil {
		if err != mgo.ErrNotFound {
			glog.Fatal("Error while getting event ", err)
		}
		return Event{}
	}
	return aggregateRecord.Events[0]
}

func NewEventStore(options *MongoEventStoreOptions) *MongoEventStore {
	return &MongoEventStore{options: options}
}
