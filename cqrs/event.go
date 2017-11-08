package cqrs

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/golang/glog"

	uuid "github.com/satori/go.uuid"
)

type EventMetaData struct {
	Id               string    `json:"id" bson:"_id"`
	OccuredAt        time.Time `json:"occured_at" bson:"occured_at"`
	AggregateVersion int       `json:"aggregate_version" bson:"aggregate_version"`
	AggregateName    string    `json:"aggregate_name" bson:"aggregate_name"`
	AggregateId      string    `json:"aggregate_id" bson:"aggregate_id"`
	Type             string    `json:"type" bson:"type"`
}

//Every action on an aggregate emits an event, which is wrapped and saved
type Event struct {
	EventMetaData `json:"event_metadata" bson:"event_metadata"`
	Payload       []byte `json:"payload" bson:"payload"`
}

func (event Event) Decode(entity interface{}) error {
	return json.Unmarshal(event.Payload, entity)
}

//Create new event
func NewEvent(aggregateId string, aggregateName string, aggregateVersion int, payload interface{}) Event {
	payloadData, err := json.Marshal(payload)
	if err != nil {
		glog.Fatal("Failed to encode payload ", err)
	}
	meta := EventMetaData{
		Id:               uuid.NewV4().String(),
		AggregateId:      aggregateId,
		AggregateVersion: aggregateVersion,
		AggregateName:    aggregateName,
		OccuredAt:        time.Now().Round(time.Millisecond),
		Type:             reflect.TypeOf(payload).String(),
	}
	return Event{meta, payloadData}
}
