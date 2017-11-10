package cqrs

import (
	"reflect"

	"github.com/golang/glog"
)

//Aggregate as in the DDD world
type Aggregate struct {
	Id      string
	name    string
	Version int
	Events  []Event
	entity  interface{}
	store   EventStore
}

type EventApplyer interface {
	ApplyEvent(event Event) error
}

type AggregateNameGetter interface {
	GetAggregateName() string
}

//Save the events accumulated so far
func (aggregate *Aggregate) Save() error {
	err := aggregate.store.SaveEvents(aggregate.Id, aggregate.Events)
	if err != nil {
		glog.Error("error while saving events for aggregate ", aggregate, err)
		return err
	}
	aggregate.Events = []Event{}
	return err
}

//Update the event
func (aggregate *Aggregate) Update(payloads ...interface{}) {
	for _, payload := range payloads {
		event := NewEvent(aggregate.Id, aggregate.name, aggregate.Version, payload)
		aggregate.Events = append(aggregate.Events, event)
		aggregate.apply(event)
	}
}

//Apply events
func (aggregate *Aggregate) apply(events ...Event) {
	for _, e := range events {
		applyer, ok := aggregate.entity.(EventApplyer)
		if ok {
			applyer.ApplyEvent(e)
		}
		aggregate.Version = e.EventMetaData.AggregateVersion + 1
	}
}

//Create new aggregate with a backing event store
func NewAggregate(id string, entity interface{}, store EventStore) Aggregate {
	name := reflect.TypeOf(entity).String()
	if nameGetter, ok := entity.(AggregateNameGetter); ok {
		name = nameGetter.GetAggregateName()
	}
	aggregate := Aggregate{
		Id:      id,
		Version: 0,
		Events:  []Event{},
		entity:  entity,
		store:   store,
		name:    name,
	}
	return aggregate
}

//Get the aggregate
func GetAggregate(id string, entity interface{}, store EventStore) Aggregate {
	aggregate := NewAggregate(id, entity, store)
	aggregate.apply(store.GetEvents(id)...)
	return aggregate
}
