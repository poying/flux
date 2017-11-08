package cqrs

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type SomeData struct {
	Data string
}

type SomePayLoad struct {
	SomeData SomeData
}

var _ = Describe("Event", func() {

	var (
		aggregateName    = "SomeAggregate"
		aggregateVersion = 1
		event            Event
	)

	BeforeEach(func() {
		event = NewEvent("agg-id", aggregateName, aggregateVersion, SomePayLoad{SomeData{"some data"}})
	})

	_ = Describe("Creating New Event", func() {
		It("Should generate valid event id", func() {
			Expect(event.Id).Should(HaveLen(36))
		})

		It("Should have aggregate name", func() {
			Expect(event.AggregateName).To(Equal(aggregateName))
		})

		It("Should have aggregate version", func() {
			Expect(event.AggregateVersion).To(Equal(aggregateVersion))
		})

		It("Should determine the event type", func() {
			Expect(event.Type).To(Equal("cqrs.SomePayLoad"))
		})
	})

})
