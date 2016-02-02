package main 

import ("fmt"
		    "encoding/csv"
		    "os"
        "encoding/json"
		    "gopkg.in/olivere/elastic.v2"
    
		)

type Customer struct{
	User string		
	Post_date string
	Message string
}

type output interface{
  csvWriter()
}

type Query struct {
  user string
  field string

}

func getwriter() *csv.Writer {
  file, err := os.Create("result.csv")
  if err != nil {
    panic(err)
  }
  writer := csv.NewWriter(file)
  return writer
}

func (c Customer) csvWriter(writer *csv.Writer) {
  data := []string{c.User, c.Post_date, c.Message}
  writer.Write(data)
  fmt.Println(data, "written")
  writer.Flush()
}

func getReport(query Query, client *elastic.Client) {
    termQuery := elastic.NewTermQuery(query.user,query.field)
    searchResult, err := client.Search().
    Index("customer").
    Query(&termQuery).
    Sort("user", true).
    Pretty(true).
    Do()
    if err != nil {
      panic(err)
    }
    writer := getwriter()
    if searchResult.Hits != nil {
      fmt.Println("Found total records", searchResult.TotalHits()) 
      for _,hit := range searchResult.Hits.Hits {
        var t Customer
        err:= json.Unmarshal(*hit.Source,&t)
        if err != nil {
          panic(err)
         }
        t.csvWriter(writer)
      }
    }else {
      fmt.Println("No Hits :(")
    }
}


func getClient() *elastic.Client {
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	return client
}



func main() {
  client := getClient()
  var q Query
  fmt.Println("Enter the search field")
  fmt.Scan(&q.user)
  fmt.Println("Enter the search string")
  fmt.Scan(&q.field)

  
  getReport(q,client)
}