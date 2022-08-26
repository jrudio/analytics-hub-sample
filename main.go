package main

import (
	"context"
	"flag"
	"fmt"

	analyticshub "google.golang.org/api/analyticshub/v1beta1"
)

var (
	projectID  string
	exchangeID string
	listingID  string
	region     string
)

func init() {
	flag.StringVar(&projectID, "project", "", "Google Cloud project id")
	flag.StringVar(&exchangeID, "exchange", "", "Analytics Hub exchange id")
	flag.StringVar(&listingID, "listing", "", "Analytics Hub listing id")
	flag.StringVar(&region, "region", "", "region of listing")
}

func main() {
	flag.Parse()

	if projectID == "" {
		fmt.Println("project id is required")

		return
	}

	if exchangeID == "" {
		fmt.Println("exchange id is required")

		return
	}

	if listingID == "" {
		fmt.Println("listing id is required")

		return
	}

	if region == "" {
		fmt.Println("region is required")

		return
	}

	ctx := context.Background()
	client, err := analyticshub.NewService(ctx)

	if err != nil {
		fmt.Printf("error creating service: %v\n", err)
		return
	}

	listingsClient := client.Projects.Locations.DataExchanges.Get(fmt.Sprintf("projects/%s/locations/%s/dataExchanges/%s/listings/%s", projectID, region, exchangeID, listingID))

	resp, err := listingsClient.Do()

	if err != nil {
		fmt.Printf("error getting listings: %v\n", err)
		return
	}

	fmt.Printf("Listing name: %s\nListing Description: %s\n", resp.DisplayName, resp.Description)
}

// TODO: add dataset from listing to project

// TODO: use bq client to list out tables

// func getLocations(client *analyticshub.Service) ([]string, error) {
// 	client.Projects.Locations.DataExchanges.Listings.List()
// }
