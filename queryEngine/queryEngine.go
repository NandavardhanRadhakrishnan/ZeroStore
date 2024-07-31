package queryEngine

import (
	"ZeroStore/storageEngine"
	"reflect"
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
	resultType interface{}
	updateFunc func(data V) V
	updateData *V
	toDelete   bool
}

func NewQueryBuilder[K comparable, V any](dt *storageEngine.DataTable[K, V]) *QueryBuilder[K, V] {
	return &QueryBuilder[K, V]{dt: dt, toDelete: false}
}

func (qb *QueryBuilder[K, V]) Select(resultType any) *QueryBuilder[K, V] {
	qb.resultType = resultType
	return qb
}

func (qb *QueryBuilder[K, V]) Where(filter func(storageEngine.DataRow[K, V]) bool) *QueryBuilder[K, V] {
	qb.filter = filter
	return qb
}

func (qb *QueryBuilder[K, V]) GetFromKeys(keys []K) *QueryBuilder[K, V] {
	qb.keys = keys
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
	qb.toDelete = true
	return qb
}

func Execute[K comparable, V any, R any](qb *QueryBuilder[K, V]) Result[R] {
	var result R

	// Fetch rows based on keys
	if qb.keys != nil {
		res := qb.dt.GetFromKeys(qb.keys)
		if res.Err != nil {
			return Result[R]{Err: res.Err}
		}

		switch any(result).(type) {
		case []K:
			keys := make([]K, len(res.Value))
			for i, row := range res.Value {
				keys[i] = row.PrimaryKey
			}
			return Result[R]{Value: any(keys).(R)}
		case []storageEngine.DataRow[K, V]:
			return Result[R]{Value: any(res.Value).(R)}
		default:
			if qb.resultType != nil {

				rType := reflect.TypeOf((*R)(nil)).Elem()
				elemType := rType.Elem()

				results := reflect.MakeSlice(rType, 0, 0)

				resChan := qb.dt.Select(qb.keys, qb.resultType)
				for res := range resChan {
					if res.Err != nil {
						return Result[R]{Err: res.Err}
					}
					val := reflect.ValueOf(res.Value).Elem()
					typedVal := reflect.New(elemType).Elem()
					typedVal.Set(val)

					results = reflect.Append(results, typedVal)
				}
				return Result[R]{Value: results.Interface().(R)}

			}
		}
	}

	// Apply filter and fetch keys
	if qb.filter != nil {
		res := qb.dt.Where(qb.filter)
		if res.Err != nil {
			return Result[R]{Err: res.Err}
		}
		qb.keys = res.Value

		// If the result type is []K, return the keys
		switch any(result).(type) {
		case []K:
			return Result[R]{Value: any(qb.keys).(R)}
		}
	}

	// Perform update with data
	if qb.updateData != nil {
		for _, key := range qb.keys {
			res := qb.dt.UpdateWithData(key, *qb.updateData)
			if res.Err != nil {
				return Result[R]{Err: res.Err}
			}
		}
	}

	// Perform update with function
	if qb.updateFunc != nil {
		for _, key := range qb.keys {
			res := qb.dt.UpdateWithFunc(key, qb.updateFunc)
			if res.Err != nil {
				return Result[R]{Err: res.Err}
			}
		}
	}

	// Perform delete
	if qb.toDelete {
		for _, key := range qb.keys {
			deleteResult := qb.dt.Delete(key)
			if deleteResult.Err != nil {
				return Result[R]{Err: deleteResult.Err}
			}
		}
	}

	return Result[R]{}
}
