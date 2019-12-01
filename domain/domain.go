package domain

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-dataframe/domain/frame"
)

type DataFrameHeader struct {
	Pivots []string
	Vals   []string
}

type DataFrameRows struct {
	Pivots []string
	Vals   []int
}

type Agg struct {
	Pivots    []string
	AggColumn int
}

type AggResultRow struct {
	Type   string   `json:"type"`
	Pivots []string `json:"pivots"`
	Value  int      `json:"value"`
}

type AggResult []AggResultRow

// Singleton frame.Dataframe
var dataFrame frame.DataFrame

// Verify if there is an instance of frame.DataFrame created
func checkInstanceDataframe() error {
	if dataFrame == nil {
		return errors.New("No instance of dataframe created")
	}
	return nil
}

// Get the instance of frame.DataFrame
func GetCreateDataFrame(pivots []string, vals []string) frame.DataFrame {
	// Check dataframe instance already exists
	if dataFrame == nil {
		dataFrame = frame.NewDataFrame(pivots, vals)
	}

	return dataFrame
}

func AddRecord(pivots []string, vals []int) error {
	if err := checkInstanceDataframe(); err != nil {
		return err
	}

	return dataFrame.AddRecord(pivots, vals)
}

func GetAllRows() ([]frame.Row, error) {
	if err := checkInstanceDataframe(); err != nil {
		return []frame.Row{}, err
	}

	return dataFrame.GetAllRecords(), nil
}

func GetRow(idx int) (frame.Row, error) {
	if err := checkInstanceDataframe(); err != nil {
		return frame.Row{}, err
	}

	row, err := dataFrame.GetRecord(idx)

	if err != nil {
		return frame.Row{}, err
	}

	return row, nil
}

func CleanDataframe() frame.DataFrame {
	dataFrame = nil
	return dataFrame
}

func createAggResult(aggType string, pivots []*frame.Pivot) AggResult {
	var aggResult AggResult
	for _, pivot := range pivots {
		aggResult = append(aggResult, AggResultRow{aggType, pivot.Pivots, pivot.Value})
	}
	return aggResult
}

func GetAggSum(options Agg) (AggResult, error) {
	if err := checkInstanceDataframe(); err != nil {
		return AggResult{}, err
	}

	agg, _ := dataFrame.Agg(options.Pivots...)
	pivots, _ := agg.Sum(options.AggColumn)

	return createAggResult("sum", pivots), nil
}

func GetAggCount(options Agg) (AggResult, error) {
	if err := checkInstanceDataframe(); err != nil {
		return AggResult{}, err
	}

	agg, _ := dataFrame.Agg(options.Pivots...)
	pivots, _ := agg.Count()

	return createAggResult("count", pivots), nil
}

func aggRun(pivots []string, aggColumn int) []AggResult {

	aggResult := make(chan AggResult)
	var groupResult []AggResult
	var wg sync.WaitGroup

	// simulates timeout
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)

	defer func() {
		fmt.Println("Finish agg")
		cancel()
	}()

	agg, _ := dataFrame.Agg(pivots...)

	wg.Add(2)

	go func() {
		log.Println("-> Go sum")

		//simulates slow agg async
		// time.Sleep(5 * time.Second)
		sum, _ := agg.Sum(aggColumn)

		select {
		case <-ctxWithTimeout.Done():
			log.Println("Timeout during sum...")
			aggResult <- AggResult{}
		default:
			aggResult <- createAggResult("sum", sum)
		}
	}()

	go func() {
		log.Println("-> Go count")
		//simulates slow count async
		// time.Sleep(5 * time.Second)
		count, _ := agg.Count()

		select {
		case <-ctxWithTimeout.Done():
			log.Println("Timeout during count...")
			aggResult <- AggResult{}
		default:
			aggResult <- createAggResult("count", count)
		}

	}()

	go func() {
		for result := range aggResult {
			groupResult = append(groupResult, result)
			wg.Done()
		}
	}()

	wg.Wait()

	log.Println("Agg done...")

	return groupResult
}

func GetAgg(options Agg) ([]AggResult, error) {

	if err := checkInstanceDataframe(); err != nil {
		return []AggResult{}, err
	}

	return aggRun(options.Pivots, options.AggColumn), nil
}
