package mongodb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/poying/flux/cqrs"
)

type EventPayload struct {
	Data string
}

func events(id string) []Event {
	e1 := NewEvent(id, "sample_aggregate", 1, EventPayload{"payload"})
	e2 := NewEvent(id, "sample_aggregate", 2, EventPayload{"payload"})
	return []Event{e1, e2}
}

var _ = Describe("MongoStore", func() {
	var store EventStore
	databaseName := "test"

	BeforeEach(func() {
		options := DefaultMongoEventStoreOptions()
		options.Session = session
		options.Database = databaseName
		store = NewEventStore(options)
	})

	AfterEach(func() {
		db := session.DB(databaseName)
		options := DefaultMongoEventStoreOptions()
		db.C(options.TransactionCollection).DropCollection()
		db.C(options.EventCollection).DropCollection()
		db.C(options.AggregateCollection).DropCollection()
	})

	It("Should store events", func() {
		err1 := store.SaveEvents("aggregate-1", events("aggregate-1"))
		err2 := store.SaveEvents("aggregate-2", events("aggregate-2"))

		Expect(err1, err2).To(BeNil())
		Expect(store.GetEvents("aggregate-1")).To(HaveLen(2))
		Expect(store.GetEvents("aggregate-2")).To(HaveLen(2))
	})

	It("Should reject events which has wrong aggregate version", func() {
		err1 := store.SaveEvents("aggregate-1", events("aggregate-1"))
		err2 := store.SaveEvents("aggregate-1", events("aggregate-1"))
		err3 := store.SaveEvents("aggregate-1", []Event{NewEvent("aggregate-1", "sample_aggregate", 3, EventPayload{"payload"})})

		Expect(err1).To(BeNil())
		Expect(err2).ShouldNot(BeNil())
		Expect(err3).To(BeNil())
	})

	var _ = Describe("Fetching all event metadata from a secific offset", func() {
		It("Should get the events", func() {
			e1 := NewEvent("aggregate1", "sample_aggregate", 1, EventPayload{"payload"})
			e2 := NewEvent("aggregate1", "sample_aggregate", 2, EventPayload{"payload"})
			e3 := NewEvent("aggregate1", "sample_aggregate", 3, EventPayload{"payload"})
			e4 := NewEvent("aggregate1", "sample_aggregate", 4, EventPayload{"payload"})

			store.SaveEvents("aggregate1", []Event{e1, e2, e3, e4})

			metas := store.GetEventMetaDataFrom(1, 2)
			Expect(metas).To(HaveLen(2))
			Expect(metas[0].Id).To(Equal(e2.Id))
			Expect(metas[1].Id).To(Equal(e3.Id))
		})

		It("Should handle count gracefully", func() {
			e1 := NewEvent("aggregate1", "sample_aggregate", 1, EventPayload{"payload"})
			e2 := NewEvent("aggregate1", "sample_aggregate", 2, EventPayload{"payload"})

			store.SaveEvents("aggregate1", []Event{e1, e2})

			metas := store.GetEventMetaDataFrom(1, 5)

			Expect(metas).To(HaveLen(1))
			Expect(metas[0].Id).To(Equal(e2.Id))
		})
	})

	It("Should deserialize payload", func() {
		err := store.SaveEvents("aggregate-1", events("aggregate-1"))
		Expect(err).To(BeNil())
		payload := &EventPayload{}
		err = store.GetEvents("aggregate-1")[0].Decode(payload)
		Expect(err).To(BeNil())
		Expect(payload.Data).To(Equal("payload"))
	})

	It("Should get specific event", func() {
		expected := NewEvent("a-id", "aid", 0, EventPayload{"payload"})
		err := store.SaveEvents("a-id", []Event{expected})

		actual := store.GetEvent(expected.Id)

		Expect(err).To(BeNil())
		Expect(expected.EventMetaData).To(Equal(actual.EventMetaData))
	})

	var _ = Describe("When aggregate does not exists", func() {
		It("Should not panic", func() {
			_ = store.GetEvents("ghost")
		})
	})

	var _ = Describe("When event does not exists", func() {
		It("Should not panic", func() {
			_ = store.GetEvent("ghost")
		})
	})
})
