package cqrs

import (
	"encoding/json"
	"reflect"
	"time"

	uuid "github.com/satori/go.uuid"
)

type EventTypeGetter interface {
	GetEventType() string
}

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
	Payload       interface{} `json:"payload" bson:"payload"`
}

func (event Event) Decode(entity interface{}) error {
	jsonStr, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonStr, entity)
}

//Create new event
func NewEvent(aggregateId string, aggregateName string, aggregateVersion int, payload interface{}) Event {
	typ := reflect.TypeOf(payload).String()
	if typeGetter, ok := payload.(EventTypeGetter); ok {
		typ = typeGetter.GetEventType()
	}
	meta := EventMetaData{
		Id:               uuid.NewV4().String(),
		AggregateId:      aggregateId,
		AggregateVersion: aggregateVersion,
		AggregateName:    aggregateName,
		OccuredAt:        time.Now().Round(time.Millisecond),
		Type:             typ,
	}
	return Event{meta, payload}
}
