package domain

import (
	"errors"

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

func createAggResult(pivots []*frame.Pivot) AggResult {
	var aggSum AggResult
	for _, pivot := range pivots {
		aggSum = append(aggSum, AggResultRow{pivot.Pivots, pivot.Value})
	}
	return aggSum
}

func GetAggSum(options Agg) (AggResult, error) {
	if err := checkInstanceDataframe(); err != nil {
		return AggResult{}, err
	}

	agg, _ := dataFrame.Agg(options.Pivots...)
	pivots, _ := agg.Sum(options.AggColumn)

	return createAggResult(pivots), nil
}

func GetAggCount(options Agg) (AggResult, error) {
	if err := checkInstanceDataframe(); err != nil {
		return AggResult{}, err
	}

	agg, _ := dataFrame.Agg(options.Pivots...)
	pivots, _ := agg.Count()

	return createAggResult(pivots), nil
}
