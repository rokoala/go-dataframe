package frame

import (
	"errors"
	"fmt"
	"strings"
)

/*
 * Um DataFrame representa uma tabela em que as colunas podem ser
 * agrupadas para gerar aggregações.
 * As colunas são separadas em colunas pivotáveis e agregáveis.
 * Por simplificação, todas as colunas pivotáveis será `string`
 * e todas as colunas agregáveis seráo inteiras.
 *
 * As agregações
 *
 *	DataFrame{["A", "B", "C"], ["V1", "V2"]}
 * 	A	B	C	V1	V2
 * --------------------
 *	a	b	c	10	100
 *	a	d	e	20	200
 *	x	y	z	30	400
 *
 * Agg{[A,B]}.Sum(0)	Agg[A].Sum(1)
 * ===============		=============
 * 	A	B	SUM			A	SUM
 *	------------		--------
 *	a	b	10			a	300
 * 	a	d	20			x	400
 *	x	y	30
 *
 * pivots := Agg[A].Sum(0)
 * =======================
 * Agg[A].Dim() == 1, Agg[A].Pivots() == ["A"]
 * Agg[A].Distinct() == 2
 *
 * 	A	SUM					Agg[A].Count
 *	--------				------------
 *	a	30		pivots[0]		2
 *	x	30		pivots[1]		1
 *
 * GetPivotOrNil(pivots, "a") == Pivot{dim: 1, typo: "SUM", pivots: ["a"], value: 30}
 * GetPivotOrNil(pivots, "x") == Pivot{dim: 1, typo: "SUM", pivots: ["x"], value: 30}
 */

type DataFrame interface {
	// As colunas pivotáveis
	PivotableColumns() []string
	// As colunas aqregáveis
	ValuableColumns() []string

	// Adiciona uma linha no DataFrame
	// Retorna error se não for do shape correto
	AddRecord(pivots []string, vals []int) error
	// Adiciona vários records ao DataFrame
	AddRecords(recs []interface{}) error

	// Retorna todos os Records
	GetAllRecords() []Row

	// Retorna record por indíce
	GetRecord(idx int) (Row, error)

	// Agregador das colunas pivots passadas.
	// pivots: nome das colunas agregadas
	Agg(pivots ...string) (Agg, error)

	/*
	 * Métodos funcionais.
	 */

	// Executa a função para cada linha do dataframe g
	Foreach(func(pivots []string, vals []int))
	// Mapeia cada linha do dataframe em um valor através da função de mapeamento.
	Map(func(pivots []string, vals []int) ([]int, error)) DataFrame
	// Retorna um novo DataFrame sem as linhas sinalizadas pela função filtro.
	Filter(func(pivots []string, vals []int) (bool, error)) DataFrame
	// Reduz todo o dataframe a um valor através da função de redução.
	// A função de redução recebe a redução das linhas anteriores e gera a próxima redução.
	// A redução retornada será a da última linha.
	// Ex. de redução: Somar todos os valores das colunas agregáveis.
	Reduce(func(acum int, pivots []string, vals []int) (int, error)) int
}

// Agg retorna um subespaço do DataFrame, onde podem ser executadas
// operações sobre as colunas numéricas.
type Agg interface {
	// Dim retorna a quantidade de colunas sumarizáveis.
	// As colunas sumarizáveis não contam as colunas numéricas.
	Dim() int
	// Pivots retorna as colunas sumarizáveis.
	Pivots() []string
	Frame() DataFrame

	// Distinct conta a quantidade de pivots diferentes.
	Distinct() int
	// Count retorna a quantidade de repetições de cada pivoteamento.
	// A ordem do array é a mesma dos pivoteamentos gerados por qualquer função de agregação.
	Count() ([]*Pivot, error)
	// Sum é uma função de agregação que retorna os pivoteamentos das colunas da agregação.
	Sum(idx int) ([]*Pivot, error)
	// No caso de agregação da média deve ser
	// arredondado o valor para um inteiro.
	Avg(idx int) ([]*Pivot, error)
	// Max é uma função de agregação que retorna os pivoteamentos das colunas da agregação.
	Max(idx int) ([]*Pivot, error)
	// Min é uma função de agregação que retorna os pivoteamentos das colunas da agregação.
	Min(idx int) ([]*Pivot, error)
}

// Pivot é a estrutura que retorna uma instância do pivoteamento
// (valores das colunas pivot e o valor de uma função de agregação).
type Pivot struct {
	dim    int
	typo   string   // O tipo: SUM | AVG | ...
	Pivots []string // O valor das colunas pivotadas
	Value  int      // O valor da operação de pivotamente sobre as colunas
}

// Estrutura de cada linha, contendo colunas pivotáveis e colunas agregáveis
type Row struct {
	Pivots []string `json:"pivots"`
	Vals   []int    `json:"vals"`
}

// Estrutura de implementação da interface Dataframe
type dataFrame struct {
	// nome das colunas pivotáveis
	namePivots []string
	// nome das colunas agregáveis
	nameVals []string
	rows     []Row
}

// Estrutura de implementação da interface Agg
type Aggregation struct {
	frame dataFrame
	dim   int
}

func (agg *Aggregation) Dim() int {
	return agg.dim
}

func (agg *Aggregation) Pivots() []string {
	return agg.frame.namePivots
}

func (agg *Aggregation) Frame() DataFrame {
	return &agg.frame
}

func (agg *Aggregation) Distinct() int {
	m := make(map[string]int)

	for _, row := range agg.frame.rows {
		m[strings.Join(row.Pivots[:], ",")] += 1
	}

	return len(m)
}

// Estrutura auxiliar necessário para manter a ordem/index nos maps
type NavigationMap struct {
	m    map[string]int
	keys []string
}

// Adiciona uma chave valor
func (n *NavigationMap) Set(k string, v int) {
	_, present := n.m[k]
	n.m[k] = v
	// Caso a chave já está presente não é adicionado como novo elemento
	if !present {
		n.keys = append(n.keys, k)
	}
}

// cria/inicializa um novo NavigationMap
func NewNavigationMap() *NavigationMap {
	var navMap NavigationMap
	navMap.m = make(map[string]int)
	navMap.keys = make([]string, 0)
	return &navMap
}

func (agg *Aggregation) Count() ([]*Pivot, error) {
	navMap := NewNavigationMap()
	pivots := []*Pivot{}

	for _, row := range agg.frame.rows {
		key := strings.Join(row.Pivots[:], ",")
		navMap.Set(key, navMap.m[key]+1)
	}

	// keys := []int{}
	for _, value := range navMap.keys {
		// keys = append(keys, navMap.m[value])
		p := strings.Split(value, ",")
		pivots = append(pivots, &Pivot{dim: len(p), typo: "COUNT", Pivots: p, Value: navMap.m[value]})
	}

	return pivots, nil
}

func checkOutOfBounds(idx int, max int) error {
	if idx < 0 || idx > max {
		return fmt.Errorf("index %d out of bounds [0:%d]", idx, max)
	}
	return nil
}

func (agg *Aggregation) Sum(idx int) ([]*Pivot, error) {

	if err := checkOutOfBounds(idx, len(agg.frame.nameVals)); err != nil {
		return nil, err
	}

	pivots := []*Pivot{}

	navMap := NewNavigationMap()

	for _, row := range agg.frame.rows {
		key := strings.Join(row.Pivots[:], ",")
		navMap.Set(key, navMap.m[key]+row.Vals[idx])
	}

	for _, value := range navMap.keys {
		p := strings.Split(value, ",")
		pivots = append(pivots, &Pivot{dim: len(p), typo: "SUM", Pivots: p, Value: navMap.m[value]})
	}

	return pivots, nil
}

func (agg *Aggregation) Avg(idx int) ([]*Pivot, error) {

	if err := checkOutOfBounds(idx, len(agg.frame.nameVals)); err != nil {
		return nil, err
	}

	pivots := []*Pivot{}

	navMap := NewNavigationMap()
	navMapCount := NewNavigationMap()

	for _, row := range agg.frame.rows {
		key := strings.Join(row.Pivots[:], ",")
		navMap.Set(key, navMap.m[key]+row.Vals[idx])
		navMapCount.Set(key, navMapCount.m[key]+1)
	}

	for _, value := range navMap.keys {
		p := strings.Split(value, ",")
		pivots = append(pivots, &Pivot{dim: len(p), typo: "AVG", Pivots: p, Value: navMap.m[value] / navMapCount.m[value]})
	}

	return pivots, nil
}

func (agg *Aggregation) Max(idx int) ([]*Pivot, error) {

	if err := checkOutOfBounds(idx, len(agg.frame.nameVals)); err != nil {
		return nil, err
	}

	var maxPivot []*Pivot

	max := agg.frame.rows[0].Vals[idx]

	for _, row := range agg.frame.rows {
		if max <= row.Vals[idx] {
			if max != row.Vals[idx] {
				maxPivot = nil
			}
			max = row.Vals[idx]
			maxPivot = append(maxPivot, &Pivot{dim: 1, typo: "MAX", Pivots: row.Pivots, Value: max})
		}
	}

	return maxPivot, nil
}

func (agg *Aggregation) Min(idx int) ([]*Pivot, error) {

	if err := checkOutOfBounds(idx, len(agg.frame.nameVals)); err != nil {
		return nil, err
	}

	var minPivot []*Pivot

	min := agg.frame.rows[0].Vals[idx]

	for _, row := range agg.frame.rows {
		if min >= row.Vals[idx] {
			if min != row.Vals[idx] {
				minPivot = nil
			}
			min = row.Vals[idx]
			minPivot = append(minPivot, &Pivot{dim: 1, typo: "MIN", Pivots: row.Pivots, Value: min})
		}
	}

	return minPivot, nil
}

func (df *dataFrame) AddRecord(pivots []string, vals []int) error {
	if len(pivots) > len(df.namePivots) {
		return fmt.Errorf("pivot length %d exceed the Dataframe pivot current size %d", len(pivots), len(df.namePivots))
	} else if len(vals) > len(df.nameVals) {
		return fmt.Errorf("vals length %d exceed the Datafram val current size %d", len(vals), len(df.nameVals))
	}

	df.rows = append(df.rows, Row{pivots, vals})
	return nil

}

func (df *dataFrame) AddRecords(recs []interface{}) error {
	var err error

	newDf := df
	for _, record := range recs {
		row, ok := record.(Row)
		if !ok {
			err = fmt.Errorf("Record %v doesn't have the Row struct: {pivots []string vals int[]}", row)
			break
		}

		if addRecordErr := newDf.AddRecord(row.Pivots, row.Vals); addRecordErr != nil {
			err = addRecordErr
			break
		}
	}
	// If there's an error on adding records, return the error
	if err != nil {
		return err
	}

	df = newDf

	return nil
}

func (df *dataFrame) GetAllRecords() []Row {
	return df.rows
}

func (df *dataFrame) GetRecord(idx int) (Row, error) {
	for i, row := range df.rows {
		if idx == i {
			return row, nil
		}
	}

	return Row{}, fmt.Errorf("Could not find index %d", idx)

}

// Obtém o index dos pivots de acordo com as strings passadas de argumento
func (df *dataFrame) GetPivotsIdx(pivots ...string) ([]int, error) {
	idx := []int{}
	found := false

	for _, pivotName := range pivots {
		found = false
		for index, dfPivotName := range df.namePivots {
			if pivotName == dfPivotName {
				idx = append(idx, index)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("Not found pivot %s", pivotName)
		}
	}

	return idx, nil
}

func NewAgg(df *dataFrame, pivot []string, vals []string) (Agg, error) {

	newDf, ok := NewDataFrame(pivot, vals).(*dataFrame)

	if !ok {
		return nil, errors.New("Error converting Type dataFrame")
	}

	pivotIdx, err := df.GetPivotsIdx(pivot...)
	if err != nil {
		return nil, err
	}

	record := []string{}
	for _, row := range df.rows {
		for _, idx := range pivotIdx {
			record = append(record, row.Pivots[idx])
		}
		newDf.AddRecord(record, row.Vals)
		record = nil
	}

	return &Aggregation{*newDf, len(pivot)}, nil
}

func (df *dataFrame) Agg(pivots ...string) (Agg, error) {
	newAgg, err := NewAgg(df, pivots, df.nameVals)
	if err != nil {
		return nil, err
	}
	return newAgg, nil
}

func (df *dataFrame) Foreach(cb func(pivots []string, vals []int)) {
	for i, _ := range df.rows {
		cb(df.rows[i].Pivots, df.rows[i].Vals)
	}
}

func (df *dataFrame) Map(cb func(pivots []string, vals []int) ([]int, error)) DataFrame {
	newDf := NewDataFrame(df.namePivots, df.nameVals)
	df.Foreach(func(pivots []string, vals []int) {
		if newVal, err := cb(pivots, vals); err == nil {
			newDf.AddRecord(pivots, newVal)
		}
	})
	return newDf
}

func (df *dataFrame) Filter(cb func(pivots []string, vals []int) (bool, error)) DataFrame {
	newDf := NewDataFrame(df.namePivots, df.nameVals)
	df.Foreach(func(pivots []string, vals []int) {
		if check, err := cb(pivots, vals); err == nil && check == true {
			newDf.AddRecord(pivots, vals)
		}
	})
	return newDf
}

func (df *dataFrame) Reduce(cb func(acum int, pivots []string, vals []int) (int, error)) int {
	// harcoded intialize in 0, should have an option to initialize accumulator by arguments
	accumulator := 0
	df.Foreach(func(pivots []string, vals []int) {
		if value, err := cb(accumulator, pivots, vals); err == nil {
			accumulator += value
		}
	})
	return accumulator
}

func (df *dataFrame) PivotableColumns() []string {
	return df.namePivots
}

func (df *dataFrame) ValuableColumns() []string {
	return df.nameVals
}

func NewDataFrame(pivots []string, vals []string) DataFrame {
	return &dataFrame{pivots, vals, nil}
}

// GetPivotOrNil é uma Função `utilitária` para retornar determinado Pivot, recebendo o array
// de pivots gerado pelo Agg.Sum, por ex.
// Params:
// 		pivots: array de Pivot gerado por uma função de agregação.
// 		pivotation, sequência de valores que identificam uma instância do pivoteamente.
func GetPivotOrNil(pivots []*Pivot, pivotation ...string) *Pivot {
	var p *Pivot

	found := false
	for _, pivot := range pivots {
		if strings.Join(pivot.Pivots, ",") == strings.Join(pivotation, ",") {
			p = pivot
			found = true
		}
	}

	if !found {
		return nil
	}
	return p
}
