package queryEngine

import (
	"ZeroStore/storageEngine"
)

type QueryBuilder[K comparable, V any] struct {
	dt         *storageEngine.DataTable[K, V]
	keys       []K
	filter     func(storageEngine.DataRow[K, V]) bool
	columns    []string
	updateFunc func(data V) V
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

func (qb *QueryBuilder[K, V]) UpdateWithFunc(updateFunc func(data V) V) *QueryBuilder[K, V] {
	qb.updateFunc = updateFunc
	return qb
}

func (qb *QueryBuilder[K, V]) Delete() *QueryBuilder[K, V] {
	return qb
}

func (qb *QueryBuilder[K, V]) Execute() ([]interface{}, error) {

	if qb.filter != nil {
		keys, err := qb.dt.Where(qb.filter)
		if err != nil {
			return nil, err
		}
		qb.keys = keys
	}

	if qb.updateFunc != nil {
		for _, key := range qb.keys {
			// TODO implement basic logging (5 rows updated)
			err := qb.dt.UpdateWithFunc(key, qb.updateFunc)
			if err != nil {
				return nil, err
			}
		}
	}

	if qb.columns != nil {
		results, err := qb.dt.Select(qb.keys, qb.columns)
		if err != nil {
			return nil, err
		}
		return results, nil
	}

	if qb.updateFunc == nil && qb.columns == nil {
		for _, key := range qb.keys {
			_, err := qb.dt.Delete(key)
			if err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}
