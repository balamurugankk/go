package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/balamurugankk/go/models"
)

type Servers struct {
	Servers []Server `json:"servers"`
}

type Server struct {
	Name    string   `json:"name"`
	Regions []Region `json:"regions"`
}

type Region struct {
	Eastred      []ER `json:"east-red"`
	Eastblack    []EB `json:"east-black"`
	Centralred   []CR `json:"central-red"`
	Centralblack []CB `json:"central-black"`
}

type ER struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type EB struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type CR struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

type CB struct {
	Nos  string `json:"nos"`
	List string `json:"list"`
}

func main() {

	fmt.Println("This my github test code")
	fmt.Println(models.Codeset2())
	codeset1()
	http.HandleFunc("/", codeset3)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func codeset3(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

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
		fmt.Println("User Type: " + servers.Servers[i].Name)
	}
}
