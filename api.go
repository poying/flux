package flux

import (
	"net/http"
	"time"

	"github.com/poying/flux/mongodb"

	"github.com/poying/flux/consumer"
	. "github.com/poying/flux/cqrs"
	"github.com/poying/flux/feed"
)

//Create a new mongo store
func NewMongoStore(options *mongodb.MongoEventStoreOptions) EventStore {
	return mongodb.NewEventStore(options)
}

//Offset store backed by mongodb
func NewMongoOffsetStore(options *mongodb.MongoOffsetStoreOptions) OffsetStore {
	return mongodb.NewOffsetStore(options)
}

//Start feeding events over the mux router
func FeedHandler(store EventStore) func(http.ResponseWriter, *http.Request) {
	return feed.FeedHandler(store)
}

//Create new consumer
func NewEventConsumer(url string, interval time.Duration, events []interface{}, store OffsetStore) EventConsumer {
	return consumer.New(url, events, store, interval)
}
