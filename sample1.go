package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/balamurugankk/go/models"
)

func main() {

	fmt.Println("This my github test code")
	fmt.Println(models.Codeset2())
	codeset1()
	http.HandleFunc("/", codeset3())
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func codeset3(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func codeset1() {
	fmt.Println("Haiiiiii")
}
