package main

import (
	"fmt"
	"log"

	"yswa-var/dhan_go_bus/dhan-redpanda-starter/internal/config"

	dhan_ops "yswa-var/dhan_go_bus/dhan-redpanda-starter/internal/dhan"
)

func main() {
	contracts, err := dhan_ops.ResolveContracts("contracts.csv")
	if err != nil {
		log.Fatal(err)
	}
	if len(contracts) != 48 {
		log.Fatalf("resolved %d instruments, want 48", len(contracts))
	}

	config, err := config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	token, err := config.GetAuthToken()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(token)

}
