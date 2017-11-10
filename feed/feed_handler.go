package feed

import (
	"encoding/json"
	"net/http"
	"strconv"

	"strings"

	. "github.com/poying/flux/cqrs"
	. "github.com/poying/flux/utils"
)

const DEFAULT_PAGE_SIZE = 20

var generator = JsonFeedGenerator{}

func events(w http.ResponseWriter, r *http.Request, store EventStore) {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	w.Header().Set("Content-Type", "application/json")
	if offset < 0 {
		w.WriteHeader(422)
		w.Write([]byte(`{"message": "Invalid offset"}`))
	}
	metas := store.GetEventMetaDataFrom(offset, DEFAULT_PAGE_SIZE)
	w.Write(generator.Generate(GetAbsoluteUrl(r), "event feed", metas))
}

func event(w http.ResponseWriter, r *http.Request, store EventStore, id string) {
	event := store.GetEvent(id)
	data, _ := json.Marshal(event.Payload)
	w.Write(data)
}

func getEventId(path string) string {
	xs := strings.Split(path, "/")
	if len(xs) == 3 && len(xs[2]) > 0 {
		return xs[2]
	}
	return ""
}

//Exposes events as atom feed
func FeedHandler(store EventStore) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.URL.Path == "/events" {
			events(w, r, store)
			return
		}
		if id := getEventId(r.URL.Path); len(id) > 0 {
			event(w, r, store, id)
		}
	}
}
