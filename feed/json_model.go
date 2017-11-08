package feed

import (
	"fmt"
	"time"

	. "github.com/poying/flux/cqrs"
)

type JsonEventFeed struct {
	Description string       `json:"description"`
	Events      []EventEntry `json:"events"`
}

type EventEntry struct {
	EventId          string    `json:"event_id"`
	Url              string    `json:"url"`
	AggregateName    string    `json:"aggregate_name"`
	AggregateVersion int       `json:"aggregate_version"`
	EventType        string    `json:"event_type"`
	Created          time.Time `json:"created"`
}

func ToEventEntry(url string, meta EventMetaData) EventEntry {
	return EventEntry{
		meta.Id,
		fmt.Sprintf("%s/%s", url, meta.Id),
		meta.AggregateName,
		meta.AggregateVersion,
		meta.Type,
		meta.OccuredAt,
	}
}
