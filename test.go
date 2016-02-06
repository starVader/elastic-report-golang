package main 

import (
		"fmt"
		"gopkg.in/olivere/elastic.v3"
		"encoding/json"
		"encoding/csv"
		"os"
		)



type Tweet struct {
	User string
	Post_date string
	Message string
}

type MyjsonStruct struct {
	Query struct {
			User string `json:"user"`
			Filter struct {
				Filtered string `json:"filter"`
			} `json:"filter"`
	} `json:"query"`
}

type Output interface{
    csvWriter()
}

func getwriter() *csv.Writer {
    file, err := os.Create("result.csv")
    if err != nil {
        panic(err)
    }
    writer := csv.NewWriter(file)
    return writer
}

func getClient() *elastic.Client {
	client, err := elastic.NewClient()
	if err != nil {
		fmt.Println(err)
	}
	return client
}

func (c Tweet) csvWriter(writer *csv.Writer) {
    data := []string{c.User, c.Post_date, c.Message}
    writer.Write(data)
    writer.Flush()
}


func getReport( client *elastic.Client) {
	searchResult, err := client.Scroll("twitter").Size(1).Do()
	if err != nil {
		panic(err)
	} 
	pages := 0
	numdocs := 0
	scrollId := searchResult.ScrollId

	writer := getwriter()
	for {
		 searchResult,err := client.Scroll("twitter").
		 	Size(1).
		 	ScrollId(scrollId).
		 	Do()
		 	if err != nil {
		 		fmt.Println(err)
		 		break
		 	}
		 	pages += 1
		 	
			for _,hit := range searchResult.Hits.Hits {
				if hit.Index != "twitter" {
					fmt.Println(hit.Index)
				}
				var t Tweet
				//item := make(map[string]interface{})
				err := json.Unmarshal(*hit.Source, &t)
				if err != nil {
					fmt.Println("failed", err)
				}
				numdocs += 1
				fmt.Println(t)
				t.csvWriter(writer)
			}
			scrollId = searchResult.ScrollId
			if scrollId == "" {
				fmt.Println(scrollId)
			}
	}

	if pages <= 0 {
		fmt.Println(pages)
	}
	fmt.Println(numdocs)

}

func main() {
	client := getClient()
	fmt.Println(client)
	/*var q MyjsonStruct
	q.Query.User = os.Args[1]
	fmt.Println(q.Query.User)
	q.Query.Filter.Filtered = os.Args[2]
	fmt.Println(q.Query.Filter.Filtered)*/
	getReport(client)
}
