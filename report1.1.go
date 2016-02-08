package main 

import (
	"encoding/csv"
	"os"
    	"encoding/json"
    	"log"
    	"io"
    	"fmt"
	"gopkg.in/olivere/elastic.v3"
    )

//Custom Loggers
var (
    Info     *log.Logger //Info
    Error    *log.Logger //Error
    )

//Initializing the loggers
func init() {
    file,err := os.OpenFile("log.txt",os.O_CREATE |os.O_WRONLY |os.O_APPEND, 0666)
    if err != nil {
        log.Fatalln("Failed to open log file")
    }
    Info = log.New(file,
        "INFO: ",
        log.Ldate |log.Ltime |log.Lshortfile)

    Error = log.New(io.MultiWriter(file, os.Stdout),  
        "ERROR: ",
        log.Ldate |log.Ltime |log.Lshortfile)
    
}

type Tweet struct{
	User string		
	Post_date string
	Message string
}

type output interface{
    csvWriter()
}

type Query struct {
    scroll_index string    // index to be scrolled
    search_type string	   // type to be scrolled
    search_field string    //search the string in type

}

func getwriter() (*csv.Writer, error) {
    file, err := os.Create("result.csv")
    writer := csv.NewWriter(file)
    Info.Println("Csv writer created")
    return writer,err
}

func (c Tweet) csvWriter(writer *csv.Writer) {
    data := []string{c.User, c.Post_date, c.Message}
    writer.Write(data)
    Info.Println(data)
    writer.Flush()
}


func getReport(query Query, client *elastic.Client) {
    searchResult, err := client.Scroll(query.scroll_index).Size(1).Do()
    if err != nil {
        panic(err)
    } 
    pages := 0
    numdocs := 0
    scroll_indexId := searchResult.ScrollId

    writer,err := getwriter()
    if err != nil {
        Info.Println("No writer")
    }
    for {
        searchResult,err := client.Scroll(query.scroll_index).
            Size(1).
            ScrollId(scroll_indexId).
            Do()
            if err != nil {
                Info.Println(err)
                break
            }
            pages += 1
            for _,hit := range searchResult.Hits.Hits {
                if hit.Index != query.scroll_index {
                    Info.Println(hit.Index)
                }
                var t Tweet
                err := json.Unmarshal(*hit.Source, &t)
                if err != nil {
                    Info.Println("failed", err)
                }
                numdocs += 1
                if t.User == query.search_field {
                    t.csvWriter(writer)
                }
            }
            scroll_indexId = searchResult.ScrollId
            if scroll_indexId == "" {
                Info.Println(scroll_indexId)
            }
    }

    if pages <= 0 {
        Info.Println(pages, "Records found")
    }
    Info.Println(numdocs)

}


func getClient() (*elastic.Client,error) {
	client, err := elastic.NewClient()
	return client,err
}



func main() {
    client, err := getClient()
    if err != nil {
        Error.Panicln(err)
        panic(err)
    }
    Info.Println("Successfully got the client:", client)
    var q Query
    
    fmt.Println("Enter the scroll_index ")
    fmt.Scan(&q.scroll_index)
    fmt.Println("Enter the search_type search_field")
    fmt.Scan(&q.search_type)
    fmt.Println("Enter the search string")
    fmt.Scan(&q.search_field)
    /*q.search_type = "search_type"  // search_type is the key
    q.search_field = "rakesh" //search_field is the value*/
    getReport(q,client)
}
