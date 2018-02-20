package gae

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"go.chromium.org/gae/impl/memory"
	"go.chromium.org/gae/service/datastore"
	"golang.org/x/net/context"
)

func Example() {
	c := memory.Use(context.Background())
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/doesntmatter", nil)
	if err != nil {
		panic(err)
	}

	innerHandler(c, w)
	fmt.Printf(string(w.Body.Bytes()))
	// Output: foo
}

func handler(w http.ResponseWriter, r *http.Request) {
	c := context.Background()
	c = prod.UseRequest(c, r)
	// add production filters, etc. here
	innerHandler(c, w)
}

type CoolStruct struct {
	ID string `gae:"$id"`

	Value string
}

func innerHandler(c context.Context, w http.ResponseWriter) {
	obj := &CoolStruct{Value: "hello"}
	if err := datastore.Put(c, obj); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "I wrote: %s", datastore.KeyForObj(c, obj))
}
