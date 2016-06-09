package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"gopkg.in/olivere/elastic.v3"
	"os"
	"reflect"
	"strings"
	"sync"
)

//Sample output struct
type Tweet struct {
	User      string
	Post_date string
	Message   string
}

//csvWriter interface
type Output interface {
	CsvWriter()
}

//Database Interface gets client according to database
type GetClient interface {
	GetClient()
}

//filter struct
type Filter struct {
	filter []Fpair
}

type Fpair struct {
	Qkey   string
	Qvalue string
}

//types of database structs
type DatabaseType int

//Constants of type DatabaseType
const (
	Elasticsearch DatabaseType = 1 + iota //Provide int values to each of constants
	Dynamo
	Mysql
)

//Enums should be able to print as strings, so we declare slice of strings
var database = [...]string{
	"Elasticsearch",
	"Dynamo",
	"Mysql",
}

//To control the default format for custom database type
func (db DatabaseType) String() string {
	return database[db-1]
}

func (typedb DatabaseType) GetClient() (interface{}, error) {
	if typedb == Elasticsearch {
		client, err := elastic.NewClient()
		return client, err
	}
	fmt.Println("No such Database", typedb)
	return nil, nil
}

func GetWriter() (*csv.Writer, error) {
	file, err := os.Create("result.csv")
	writer := csv.NewWriter(file)
	return writer, err
}

func (c Tweet) CsvWriter(writer *csv.Writer, m chan Tweet) {
	var mutex = &sync.Mutex{}
	for i := range m {
		c = i
		//fmt.Println(c)
		data := []string{c.User, c.Post_date, c.Message}
		//Introduced locks for write to csv file
		mutex.Lock()
		writer.Write(data)
		writer.Flush()
		mutex.Unlock()
		//lock closed
	}
}

//Now Generic lookup is possible thanks to this function
func GetField(v *Tweet, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	// fmt.Println(string(f.String()))
	return string(f.String())
}

//gets the searchresult and filters them for writing to csv
func Filtering(search chan *elastic.SearchResult) {
	var t Tweet
	var data chan Tweet = make(chan Tweet)
	writer, err := GetWriter()
	CheckError(err)
	go t.CsvWriter(writer, data) // spawning the csvwriter routine
	for i := range search {
		searchResult := i
		for _, hit := range searchResult.Hits.Hits {
			err := json.Unmarshal(*hit.Source, &t)
			CheckError(err)
			//Filtering data
			q.filter[0].Qkey = strings.Replace(q.filter[0].Qkey, q.filter[0].Qkey[:1], strings.ToUpper(q.filter[0].Qkey[:1]), 1)
			if GetField(&t, q.filter[0].Qkey) == q.filter[0].Qvalue {
				fmt.Println(t)
				data <- t
			}
		}
	}
	close(data) // closing the channel
}

//Scrolls elasticsearch like cursors in SQL
func GetReportEL(client *elastic.Client) {
	result := make(chan *elastic.SearchResult)
	// spawinng the Filtering routine
	go Filtering(result)
	// the termquery uses all lower but for matching to filter exactly we have to convert the first letter to upper
	boolq := elastic.NewBoolQuery()
	termQuery := boolq.Filter(elastic.NewTermQuery(q.filter[0].Qkey, q.filter[0].Qvalue))
	count, err := client.Count().
		Query(termQuery).
		Do()
	CheckError(err)
	//Gives count of total records found
	fmt.Println("Count", count)
	scrollService := elastic.NewScrollService(client)
	searchResult, err := scrollService.Scroll("5m").Size(1).Do()
	CheckError(err)
	pages := 0
	scroll_indexId := searchResult.ScrollId
	for {
		searchResult, err := scrollService.Query(termQuery).Scroll("5m").
			Size(1).
			ScrollId(scroll_indexId).
			Do()
		if err != nil {
			break
		}
		result <- searchResult // sending data into channel received by Filtering function
		pages += 1
		scroll_indexId = searchResult.ScrollId
		if scroll_indexId == "" {
			fmt.Println(scroll_indexId)
		}
	}

	if pages <= 0 {
		fmt.Println(pages, "Records found")
	}
	close(result) //closing the channel

}

func CheckError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

var q Filter // Global because it has to be used at different routines

func main() {
	var k Fpair
	var str DatabaseType
	fmt.Println("Select Database :->\n 1. For Elasticsearch\n", "2. For Dynamo\n", "3. For Mysql\n", "Enter choice")
	fmt.Scan(&str)
	// fmt.Println(reflect.TypeOf(str))
	fmt.Println("Enter the search Field")
	fmt.Scan(&k.Qkey)

	fmt.Println("Enter the search string")
	fmt.Scan(&k.Qvalue)

	q = Filter{filter: []Fpair{k}}
	fmt.Println(q.filter[0])
	client, err := str.GetClient()
	CheckError(err)
	//type assertion for getclient
	switch v := client.(type) {
	case *elastic.Client:
		fmt.Println("Calling With Elasticsearch Client")
		GetReportEL(v)
	default:
		fmt.Println("No such Client available", v)
	}
}
