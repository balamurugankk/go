package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/balamurugankk/go/models"
)

type Servers struct {
	Servers []Server `json:"servers"`
}

type Server struct {
	Name   string  `json:"name"`
	Region Regions `json:"regions"`
}

type Regions struct {
	Eastred      Eastred      `json:"eastred"`
	Eastblack    Eastblack    `json:"eastblack"`
	Centralred   Centralred   `json:"centralred"`
	Centralblack Centralblack `json:"centralblack"`
}

type Eastred struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type Eastblack struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type Centralred struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type Centralblack struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

func main() {

	fmt.Println("This my github test code")
	fmt.Println(models.Codeset2())
	codeset1()
	//http.HandleFunc("/", codeset3)
	//log.Fatal(http.ListenAndServe(":8080", nil))

}

//func codeset3(w http.ResponseWriter, r *http.Request) {
//	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
//}

func codeset1() {
	jsonFile, err := os.Open("server.json")

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var servers Servers

	json.Unmarshal(byteValue, &servers)

	for i := 0; i < len(servers.Servers); i++ {
		fmt.Println("Application Type: " + servers.Servers[i].Name)
		fmt.Println(servers.Servers[i].Region.Eastred.Nos)
		fmt.Println(servers.Servers[i].Region.Eastred.List)
	}

}
