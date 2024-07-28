package queryEngine

import (
	"ZeroStore/storageEngine"
	"fmt"
)

type Result[T any] struct {
	Value T
	Err   error
}

func NewResult[T any](value T, err error) Result[T] {
	return Result[T]{Value: value, Err: err}
}

type QueryBuilder[K comparable, V any] struct {
	dt         *storageEngine.DataTable[K, V]
	keys       []K
	filter     func(storageEngine.DataRow[K, V]) bool
	columns    []string
	updateFunc func(data V) V
	updateData *V
}

func NewQueryBuilder[K comparable, V any](dt *storageEngine.DataTable[K, V]) *QueryBuilder[K, V] {
	return &QueryBuilder[K, V]{dt: dt}
}

func (qb *QueryBuilder[K, V]) Select(columns []string) *QueryBuilder[K, V] {
	qb.columns = columns
	return qb
}

func (qb *QueryBuilder[K, V]) Where(filter func(storageEngine.DataRow[K, V]) bool) *QueryBuilder[K, V] {
	qb.filter = filter
	return qb
}

func (qb *QueryBuilder[K, V]) UpdateWithData(UpdateData V) *QueryBuilder[K, V] {
	qb.updateData = &UpdateData
	return qb
}

func (qb *QueryBuilder[K, V]) UpdateWithFunc(updateFunc func(data V) V) *QueryBuilder[K, V] {
	qb.updateFunc = updateFunc
	return qb
}

func (qb *QueryBuilder[K, V]) Delete() *QueryBuilder[K, V] {
	return qb
}

func (qb *QueryBuilder[K, V]) Execute() ([]map[string]interface{}, error) {
	if qb.filter != nil {
		res := qb.dt.Where(qb.filter)
		if res.Err != nil {
			return nil, res.Err
		}
		qb.keys = res.Value
	}

	if qb.updateData != nil {
		for _, key := range qb.keys {
			res := qb.dt.UpdateWithData(key, *qb.updateData)
			if res.Err != nil {
				return nil, res.Err
			}
		}
	}

	if qb.updateFunc != nil {
		for _, key := range qb.keys {
			// TODO implement basic logging (e.g., "5 rows updated")
			res := qb.dt.UpdateWithFunc(key, qb.updateFunc)
			if res.Err != nil {
				return nil, res.Err
			}
		}
	}

	if qb.columns != nil {
		var results []map[string]interface{}
		resChan := qb.dt.Select(qb.keys, qb.columns)
		for res := range resChan {
			if res.Err != nil {
				return nil, res.Err
			}
			row, ok := res.Value.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected type %T, expected map[string]interface{}", res.Value)
			}
			results = append(results, row)
		}
		return results, nil
	}

	if qb.updateFunc == nil && qb.columns == nil {
		for _, key := range qb.keys {
			deleteResult := qb.dt.Delete(key)
			if deleteResult.Err != nil {
				return nil, deleteResult.Err
			}
		}
	}
	return nil, nil
}
