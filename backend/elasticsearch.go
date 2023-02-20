package backend

import (
	"around/util"
	"context"
	"fmt"

	"around/constants"
	"github.com/olivere/elastic/v7"
)

// global: sessiofactory object
var (
	ESBackend *ElasticsearchBackend // capitalized "E" means global
	// Only one "sessionFactory", and the type is pointer
)
// global: sessiofactory class
// global session factory (Capitalized)
type ElasticsearchBackend struct {
	// field
	client *elastic.Client
	// name  // type is pointer
}


// method 1 of ESBackend *ElasticsearchBackend
func (backend *ElasticsearchBackend) ReadFromES(query elastic.Query, index string) (*elastic.SearchResult, error) {
	searchResult, err := backend.client.Search().
		Index(index).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	return searchResult, nil
}


// method 2 of ESBackend *ElasticsearchBackend
func (backend *ElasticsearchBackend) SaveToES(i interface{}, index string, id string) error {
	_, err := backend.client.Index().
		Index(index).
		Id(id).
		BodyJson(i).
		Do(context.Background())
	return err
}


// method 3 of ESBackend *ElasticsearchBackend
func (backend *ElasticsearchBackend) DeleteFromES() {
}



// Constructor  Init ESBackend *ElasticsearchBackend
// Go has no constructor
// We need Initiate a object
// below is the constructor of ElasticsearchBackend
func InitElasticsearchBackend(config *util.ElasticsearchInfo) {

	// new ElasticsearchBackend object

	client, err := elastic.NewClient(
		elastic.SetURL(config.Address),
		elastic.SetBasicAuth(config.Username, config.Password))
	if err != nil {
		panic(err)
	}

	// if POST_INDEX exists
	exists, err := client.IndexExists(constants.POST_INDEX).Do(context.Background())
	// if backend is too slow,
	// we can put a deadline in the context
	// to deal with error
	if err != nil {
		panic(err)
	}

	if !exists {
		mapping := `{
            "mappings": {
                "properties": {
                    "id":       { "type": "keyword" },
                    "user":     { "type": "keyword" },
                    "message":  { "type": "text" },
                    "url":      { "type": "keyword", "index": false },
                    "type":     { "type": "keyword", "index": false }
                }
            }
        }`
		_, err := client.CreateIndex(constants.POST_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}


	// if USER_INDEX exists
	exists, err = client.IndexExists(constants.USER_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if !exists {
		mapping := `{
                        "mappings": {
                                "properties": {
                                        "username": {"type": "keyword"},
                                        "password": {"type": "keyword"},
                                        "age":      {"type": "long", "index": false},
                                        "gender":   {"type": "keyword", "index": false}
                                }
                        }
                }`
		_, err = client.CreateIndex(constants.USER_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Indexes are created.")

	ESBackend = &ElasticsearchBackend{client: client}
}


