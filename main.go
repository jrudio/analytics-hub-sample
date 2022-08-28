package main

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/bigquery"
	analyticshub "google.golang.org/api/analyticshub/v1beta1"
	"google.golang.org/api/iterator"
)

var (
	projectID       string
	exchangeID      string
	listingID       string // listingID == dataset name
	region          string
	showListingInfo bool
	showTables      bool
	showDatasets    bool
)

func init() {
	flag.StringVar(&projectID, "project", "", "Google Cloud project id")
	flag.StringVar(&exchangeID, "exchange", "", "Analytics Hub exchange id")
	flag.StringVar(&listingID, "listing", "", "Analytics Hub listing id")
	flag.StringVar(&region, "region", "", "region of listing")
	flag.BoolVar(&showTables, "show-tables", false, "show available bigquery tables")
	flag.BoolVar(&showDatasets, "show-datasets", false, "show available bigquery datasets")
	flag.BoolVar(&showListingInfo, "show-listing", false, "show info from a listing on Analytics Hub")
}

func main() {
	flag.Parse()

	if !showDatasets && showTables {
		if err := printTables(); err != nil {
			fmt.Printf("failed to list tables from listing: %v\n", err)
		}
		return
	}

	if showDatasets && !showTables && !showListingInfo {
		if err := printDatasets(); err != nil {
			fmt.Printf("failed to list available datasets: %v\n", err)
		}

		return
	}

	if !showDatasets && showTables && !showListingInfo {
		if err := printTables(); err != nil {
			fmt.Printf("failed to list tables from listing: %v\n", err)
		}
		return
	}

	if !showDatasets && !showTables && showListingInfo {
		if err := printListingInfo(); err != nil {
			fmt.Printf("failed to list available datasets: %v\n", err)
		}

		return
	}

	fmt.Println("invalid selection")
	flag.Usage()
}

// printTables prints the tables available to the user within the target project
// requires the 'BigQuery User' IAM permission
func printTables() error {
	if projectID == "" {
		return fmt.Errorf("project id is required")
	}

	if listingID == "" {
		return fmt.Errorf("listing id is required")
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)

	if err != nil {
		return err
	}

	tableIterator := client.Dataset(listingID).Tables(ctx)

	fmt.Printf("listing all tables available from dataset '%s'...\n", listingID)

	hasTables := false

	for {
		t, err := tableIterator.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("table iterator error: %v", err)
		}

		fmt.Printf("- %s\n", t.TableID)

		if !hasTables {
			hasTables = true
		}
	}

	fmt.Println("finished listing all available tables")

	return nil
}

func printDatasets() error {
	if projectID == "" {
		return fmt.Errorf("project id is required")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)

	if err != nil {
		return fmt.Errorf("failed creating bq client: %v", err)
	}

	it := client.Datasets(ctx)

	fmt.Printf("printing bigquery datasets available to you from project '%s'...\n", projectID)

	hasDatasets := false

	for {
		ds, err := it.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to list datasets: %v", err)
		}

		fmt.Printf("- %s\n", ds.DatasetID)

		if !hasDatasets {
			hasDatasets = true
		}
	}

	if !hasDatasets {
		return fmt.Errorf("no datasets found")
	}

	fmt.Println("finished printing bigquery datasets...")

	return nil
}

func printListingInfo() error {
	if exchangeID == "" {
		return fmt.Errorf("exchange id is required")
	}

	if region == "" {
		return fmt.Errorf("region is required")
	}

	if listingID == "" {
		return fmt.Errorf("listing is required")
	}

	ctx := context.Background()
	client, err := analyticshub.NewService(ctx)

	if err != nil {
		return fmt.Errorf("error creating service: %v", err)
	}

	listingsClient := client.Projects.Locations.DataExchanges.Get(fmt.Sprintf("projects/%s/locations/%s/dataExchanges/%s/listings/%s", projectID, region, exchangeID, listingID))

	resp, err := listingsClient.Do()

	if err != nil {
		return fmt.Errorf("error getting listings: %v", err)
	}

	fmt.Printf("Listing name: %s\nListing Description: %s\n", resp.DisplayName, resp.Description)

	return nil
}

// func getLocations(client *analyticshub.Service) ([]string, error) {
// 	client.Projects.Locations.DataExchanges.Listings.List()
// }
