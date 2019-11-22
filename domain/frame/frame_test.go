package frame

/*
 * Os testes unitários devem cobrir os principais métodos do package,
 * de forma a ter uma cobertura de, no mínimo, 60%, alferidos com:
 *      `go test -cover`.
 *
 * Os testes unitários pode ser estruturados como melhor convier ao
 * desenvolvedor, podendo ser implementado mais de um método de testes,
 * utilizados métodos e estruturas de suporte.
 *
 * Exige-se o uso do pacote: `github.com/stretchr/testify/assert`, para
 * testar valores.
 */

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Testar um ou mais métodos do DataFrame.
func TestAggDataFrame(t *testing.T) {
	assert := assert.New(t)

	// Cria um dataframe com duas colunas pivotáveis, e duas colunas de valores
	df := NewDataFrame([]string{"A", "B", "C"}, []string{"V1", "V2"})
	// Verifica adição de novo record com Shape correto
	err := df.AddRecord([]string{"a", "b", "c"}, []int{10, 100})
	assert.Nil(err)

	//  Verifica adição de novo record com Shape incorreto
	err = df.AddRecord([]string{"a", "b", "c", "d"}, []int{0, 0})
	assert.NotNil(err)

	// Adiciona alguns records
	df.AddRecord([]string{"a", "d", "c"}, []int{10, 100})
	df.AddRecord([]string{"x", "y", "z"}, []int{20, 200})
	df.AddRecord([]string{"i", "j", "k"}, []int{40, 400})
	df.AddRecord([]string{"i", "j", "k"}, []int{30, 400})

	// Realiza agrupamento por colunas "A" e "B"
	agg, err := df.Agg("A", "B")
	// Verifica se não ocorre erro na agregação
	assert.Nil(err)

	agg2, err2 := df.Agg("A", "C")
	assert.Nil(err2)
	assert.NotNil(agg2)

	// Verifica erro ao agregar por coluna inválida
	agg3, err3 := df.Agg("A", "D")
	assert.NotNil(err3)
	assert.Nil(agg3)

	// Testa a soma da coluna V3 inexistente
	pivots, err := agg.Sum(3)
	assert.NotNil(err)

	// Testa a soma da coluna V1
	pivots, _ = agg.Sum(0)
	v1_ab := GetPivotOrNil(pivots, "a", "b")
	assert.Equal(10, v1_ab.Value)

	// Testa a soma da coluna V2
	pivots, _ = agg.Sum(1)
	v2_ab := GetPivotOrNil(pivots, "a", "b")
	assert.Equal(100, v2_ab.Value)

	// Testa a média da coluna V1
	pivots, _ = agg.Avg(0)
	avg_v1 := GetPivotOrNil(pivots, "i", "j")
	assert.Equal(35, avg_v1.Value)

	// Testa max da coluna v1
	pivots, _ = agg.Max(0)
	max_v1 := GetPivotOrNil(pivots, "i", "j")
	assert.Equal(40, max_v1.Value)

	// Testa mínimo da coluna v1
	pivots, _ = agg.Min(0)
	expected := [][]string{{"a", "b"}, {"a", "d"}}
	for i, pivot := range pivots {
		assert.Equal(expected[i], pivot.Pivots)
	}
	// Testa pivot não existente
	min_v1 := GetPivotOrNil(pivots, "i", "j")
	var p *Pivot
	assert.Equal(p, min_v1)

}

// Testa os métodos funcionais
func TestFuncDataFrame(t *testing.T) {
	assert := assert.New(t)

	// Verifica a criação de df
	df := NewDataFrame([]string{"A", "B", "C"}, []string{"V1", "V2"})

	// Verifica a adição de records
	df.AddRecord([]string{"a", "b", "c"}, []int{10, 100})
	df.AddRecord([]string{"a", "d", "c"}, []int{10, 100})
	df.AddRecord([]string{"x", "y", "z"}, []int{20, 200})
	df.AddRecord([]string{"i", "j", "k"}, []int{30, 300})
	df.AddRecord([]string{"i", "j", "k"}, []int{40, 400})

	pivotableColumns := df.PivotableColumns()
	// Verifica se o slice de strings está correto
	expected := []string{"A", "B", "C"}
	assert.Equal(expected, pivotableColumns)

	valuableColumns := df.ValuableColumns()
	assert.Equal(2, len(valuableColumns))
	// Verifica se o slice de strings
	expected = []string{"V1", "V2"}
	assert.Equal(expected, valuableColumns)

	// Testa funcionalidade forEach
	count := 0
	df.Foreach(func(pivots []string, vals []int) {
		assert.Equal([][]string{
			[]string{"a", "b", "c"},
			[]string{"a", "d", "c"},
			[]string{"x", "y", "z"},
			[]string{"i", "j", "k"},
			[]string{"i", "j", "k"}}[count], pivots)

		assert.Equal([]int{10, 10, 20, 30, 40}[count], vals[0])
		assert.Equal([]int{100, 100, 200, 300, 400}[count], vals[1])
		count++
	})

	// Transforma o df dividindo a segunda coluna por 10
	// o que resulta em um df com os valores das colunas iguais.
	df.Map(func(pivots []string, vals []int) ([]int, error) {
		return []int{vals[0], int(vals[1] / 10)}, nil
	}).Foreach(func(pivots []string, vals []int) {
		assert.Equal(vals[0], vals[1])
	})

	// Transforma o df removendo as linhas que contiverem a primeira coluna
	// menor que 20 o que resulta de um dataframe com 3 linhas
	newDf := df.Filter(func(pivots []string, vals []int) (bool, error) {
		if vals[0] <= 20 {
			return true, nil
		}
		return false, nil
	})

	// verifica se retornou o um novo DataFrame
	assert.NotNil(newDf)

	count = 0
	newDf.Foreach(func(pivots []string, vals []int) {
		assert.Equal([][]string{
			[]string{"a", "b", "c"},
			[]string{"a", "d", "c"},
			[]string{"x", "y", "z"}}[count], pivots)
		assert.Equal([]int{10, 10, 20}[count], vals[0])
		assert.Equal([]int{100, 100, 200}[count], vals[1])
		count++
	})
	assert.Equal(3, count)

	// Retorna a soma da primeira coluna utilizando reduce
	sumV1 := df.Reduce(func(accum int, pivots []string, vals []int) (int, error) {
		return vals[0], nil
	})
	assert.Equal(110, sumV1)

	// Retorna a soma da segunda coluna utilizando reduce
	sumV2 := df.Reduce(func(accum int, pivots []string, vals []int) (int, error) {
		return vals[1], nil
	})
	assert.Equal(1100, sumV2)

	// Adiciona dois records ao df
	var rows []interface{}
	rows = append(rows, Row{[]string{"a", "b", "c", "d"}, []int{10, 100, 200}})
	rows = append(rows, Row{[]string{"a", "d", "c", "d"}, []int{10, 100, 200}})
	df2 := NewDataFrame([]string{"A", "B", "C"}, []string{"V1", "V2"})
	df2.AddRecords(rows)

	count = 0
	df2.Foreach(func(pivots []string, vals []int) {
		assert.Equal([][]string{
			[]string{"a", "b", "c", "d"},
			[]string{"a", "d", "c", "d"}}[count], pivots)
		assert.Equal([]int{10, 100, 200}[count], vals[0])
		assert.Equal([]int{10, 100, 200}[count], vals[1])
		count++
	})

	// Caso ocorra algum erro na inserção dos records, nenhum record deverá ser inserido
	rows = nil
	rows = append(rows, Row{[]string{"a", "b", "c", "d"}, []int{10, 100, 200}})
	rows = append(rows, Row{[]string{"a", "d", "c", "d", "e"}, []int{10, 100, 200}})
	df3 := NewDataFrame([]string{"A", "B", "C"}, []string{"V1", "V2"})
	err := df3.AddRecords(rows)

	// Verifica que ocorre erro e nenhum record deverá ser inserido no df
	if assert.NotNil(err) {
		count = 0
		df3.Foreach(func(pivots []string, vals []int) {
			count++
		})
		assert.Equal(count, 0)
	}

}
