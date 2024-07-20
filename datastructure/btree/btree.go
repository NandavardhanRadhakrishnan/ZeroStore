package btree

import (
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

type BTreeNode[K any, V any] struct {
	IsLeaf   bool
	Keys     []K
	Children []*BTreeNode[K, V]
	Values   []V
}

type BTree[K any, V any] struct {
	root    *BTreeNode[K, V]
	t       int
	compare func(a, b K) int
}

func NewBTree[K any, V any](t int, compare func(a, b K) int) *BTree[K, V] {
	gob.Register(&BTreeNode[K, V]{})
	return &BTree[K, V]{t: t, root: nil, compare: compare}
}

func (bt *BTree[K, V]) Insert(key K, value V) {
	if bt.root == nil {
		bt.root = &BTreeNode[K, V]{IsLeaf: true}
		bt.root.Keys = []K{key}
		bt.root.Values = []V{value}
		return
	}

	if len(bt.root.Keys) == (2*bt.t)-1 {
		newRoot := &BTreeNode[K, V]{IsLeaf: false}
		newRoot.Children = append(newRoot.Children, bt.root)
		bt.splitChild(newRoot, 0)
		bt.root = newRoot
	}

	bt.insertNonFull(bt.root, key, value, bt.compare)
}

func (bt *BTree[K, V]) insertNonFull(node *BTreeNode[K, V], key K, value V, compare func(a, b K) int) {
	i := len(node.Keys) - 1

	if node.IsLeaf {
		node.Keys = append(node.Keys, key)
		node.Values = append(node.Values, value)
		for i >= 0 && compare(key, node.Keys[i]) < 0 {
			node.Keys[i+1] = node.Keys[i]
			node.Values[i+1] = node.Values[i]
			i--
		}
		node.Keys[i+1] = key
		node.Values[i+1] = value
	} else {
		for i >= 0 && compare(key, node.Keys[i]) < 0 {
			i--
		}
		i++
		if len(node.Children[i].Keys) == (2*bt.t)-1 {
			bt.splitChild(node, i)
			if compare(key, node.Keys[i]) > 0 {
				i++
			}
		}
		bt.insertNonFull(node.Children[i], key, value, compare)
	}
}

func (bt *BTree[K, V]) splitChild(parent *BTreeNode[K, V], i int) {
	t := bt.t
	child := parent.Children[i]
	newChild := &BTreeNode[K, V]{IsLeaf: child.IsLeaf}
	parent.Keys = append(parent.Keys[:i], append([]K{child.Keys[t-1]}, parent.Keys[i:]...)...)
	parent.Values = append(parent.Values[:i], append([]V{child.Values[t-1]}, parent.Values[i:]...)...)
	parent.Children = append(parent.Children[:i+1], append([]*BTreeNode[K, V]{newChild}, parent.Children[i+1:]...)...)

	newChild.Keys = make([]K, t-1)
	newChild.Values = make([]V, t-1)

	copy(newChild.Keys, child.Keys[t:])
	copy(newChild.Values, child.Values[t:])
	child.Keys = child.Keys[:t-1]
	child.Values = child.Values[:t-1]

	if !child.IsLeaf {
		newChild.Children = make([]*BTreeNode[K, V], t)
		copy(newChild.Children, child.Children[t:])
		child.Children = child.Children[:t]
	}
}

func (bt *BTree[K, V]) Search(key K) (V, bool) {
	return bt.search(bt.root, key)
}

func (bt *BTree[K, V]) search(node *BTreeNode[K, V], key K) (V, bool) {
	if node == nil {
		var zero V
		return zero, false
	}

	i := 0
	for i < len(node.Keys) && bt.compare(key, node.Keys[i]) > 0 {
		i++
	}

	if i < len(node.Keys) && bt.compare(key, node.Keys[i]) == 0 {
		return node.Values[i], true
	}

	if node.IsLeaf {
		var zero V
		return zero, false
	}

	return bt.search(node.Children[i], key)
}

func (bt *BTree[K, V]) Delete(key K) (V, bool) {
	if bt.root == nil {
		var zero V
		return zero, false
	}

	deletedValue, found := bt.delete(bt.root, key)
	if found && len(bt.root.Keys) == 0 {
		if bt.root.IsLeaf {
			bt.root = nil
		} else {
			bt.root = bt.root.Children[0]
		}
	}

	return deletedValue, found
}

func (bt *BTree[K, V]) delete(node *BTreeNode[K, V], key K) (V, bool) {
	var zero V
	t := bt.t
	idx := 0
	for idx < len(node.Keys) && bt.compare(node.Keys[idx], key) < 0 {
		idx++
	}

	if idx < len(node.Keys) && bt.compare(node.Keys[idx], key) == 0 {
		if node.IsLeaf {
			deletedValue := node.Values[idx]
			node.Keys = append(node.Keys[:idx], node.Keys[idx+1:]...)
			node.Values = append(node.Values[:idx], node.Values[idx+1:]...)
			return deletedValue, true
		} else {
			if len(node.Children[idx].Keys) >= t {
				predKey, predVal := bt.getPred(node, idx)
				node.Keys[idx] = predKey
				node.Values[idx] = predVal
				return bt.delete(node.Children[idx], predKey)
			} else if len(node.Children[idx+1].Keys) >= t {
				succKey, succVal := bt.getSucc(node, idx)
				node.Keys[idx] = succKey
				node.Values[idx] = succVal
				return bt.delete(node.Children[idx+1], succKey)
			} else {
				bt.merge(node, idx)
				return bt.delete(node.Children[idx], key)
			}
		}
	} else {
		if node.IsLeaf {
			return zero, false
		}

		flag := (idx == len(node.Keys))

		if len(node.Children[idx].Keys) < t {
			bt.fill(node, idx)
		}

		if flag && idx > len(node.Keys) {
			return bt.delete(node.Children[idx-1], key)
		} else {
			return bt.delete(node.Children[idx], key)
		}
	}
}

func (bt *BTree[K, V]) getPred(node *BTreeNode[K, V], idx int) (K, V) {
	cur := node.Children[idx]
	for !cur.IsLeaf {
		cur = cur.Children[len(cur.Keys)]
	}
	return cur.Keys[len(cur.Keys)-1], cur.Values[len(cur.Values)-1]
}

func (bt *BTree[K, V]) getSucc(node *BTreeNode[K, V], idx int) (K, V) {
	cur := node.Children[idx+1]
	for !cur.IsLeaf {
		cur = cur.Children[0]
	}
	return cur.Keys[0], cur.Values[0]
}

func (bt *BTree[K, V]) merge(node *BTreeNode[K, V], idx int) {
	child := node.Children[idx]
	sibling := node.Children[idx+1]

	child.Keys = append(child.Keys, node.Keys[idx])
	child.Values = append(child.Values, node.Values[idx])

	child.Keys = append(child.Keys, sibling.Keys...)
	child.Values = append(child.Values, sibling.Values...)

	if !child.IsLeaf {
		child.Children = append(child.Children, sibling.Children...)
	}

	node.Keys = append(node.Keys[:idx], node.Keys[idx+1:]...)
	node.Values = append(node.Values[:idx], node.Values[idx+1:]...)
	node.Children = append(node.Children[:idx+1], node.Children[idx+2:]...)
}

func (bt *BTree[K, V]) fill(node *BTreeNode[K, V], idx int) {
	if idx != 0 && len(node.Children[idx-1].Keys) >= bt.t {
		bt.borrowFromPrev(node, idx)
	} else if idx != len(node.Keys) && len(node.Children[idx+1].Keys) >= bt.t {
		bt.borrowFromNext(node, idx)
	} else {
		if idx != len(node.Keys) {
			bt.merge(node, idx)
		} else {
			bt.merge(node, idx-1)
		}
	}
}

func (bt *BTree[K, V]) borrowFromPrev(node *BTreeNode[K, V], idx int) {
	child := node.Children[idx]
	sibling := node.Children[idx-1]

	child.Keys = append([]K{node.Keys[idx-1]}, child.Keys...)
	child.Values = append([]V{node.Values[idx-1]}, child.Values...)

	if !child.IsLeaf {
		child.Children = append([]*BTreeNode[K, V]{sibling.Children[len(sibling.Children)-1]}, child.Children...)
	}

	node.Keys[idx-1] = sibling.Keys[len(sibling.Keys)-1]
	node.Values[idx-1] = sibling.Values[len(sibling.Values)-1]

	sibling.Keys = sibling.Keys[:len(sibling.Keys)-1]
	sibling.Values = sibling.Values[:len(sibling.Values)-1]

	if !sibling.IsLeaf {
		sibling.Children = sibling.Children[:len(sibling.Children)-1]
	}
}

func (bt *BTree[K, V]) borrowFromNext(node *BTreeNode[K, V], idx int) {
	child := node.Children[idx]
	sibling := node.Children[idx+1]

	child.Keys = append(child.Keys, node.Keys[idx])
	child.Values = append(child.Values, node.Values[idx])

	if !child.IsLeaf {
		child.Children = append(child.Children, sibling.Children[0])
		sibling.Children = sibling.Children[1:]
	}

	node.Keys[idx] = sibling.Keys[0]
	node.Values[idx] = sibling.Values[0]

	sibling.Keys = sibling.Keys[1:]
	sibling.Values = sibling.Values[1:]
}

func (bt *BTree[K, V]) PrettyPrint() {
	bt.prettyPrint(bt.root, 0)
}

func (bt *BTree[K, V]) prettyPrint(node *BTreeNode[K, V], level int) {
	if node == nil {
		return
	}

	indent := strings.Repeat("  ", level)
	fmt.Printf("%sLevel %d: ", indent, level)
	for i, key := range node.Keys {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%v", key)
	}
	fmt.Println()

	if !node.IsLeaf {
		for _, child := range node.Children {
			bt.prettyPrint(child, level+1)
		}
	}
}

func (bt *BTree[K, V]) Save(file *os.File) error {
	defer file.Close()

	encoder := gob.NewEncoder(file)

	var saveNode func(node *BTreeNode[K, V]) error
	saveNode = func(node *BTreeNode[K, V]) error {
		if node == nil {
			if err := encoder.Encode(false); err != nil {
				return err
			}
			return nil
		}
		if err := encoder.Encode(true); err != nil {
			return err
		}
		if err := encoder.Encode(node); err != nil {
			return err
		}
		for _, child := range node.Children {
			if err := saveNode(child); err != nil {
				return err
			}
		}
		return nil
	}

	if err := saveNode(bt.root); err != nil {
		return err
	}

	return nil
}

func (bt *BTree[K, V]) Load(file *os.File) error {
	defer file.Close()

	decoder := gob.NewDecoder(file)

	var loadNode func() (*BTreeNode[K, V], error)
	loadNode = func() (*BTreeNode[K, V], error) {
		var notNil bool
		if err := decoder.Decode(&notNil); err != nil {
			return nil, err
		}
		if !notNil {
			return nil, nil
		}
		node := &BTreeNode[K, V]{}
		if err := decoder.Decode(node); err != nil {
			return nil, err
		}
		for i := range node.Children {
			child, err := loadNode()
			if err != nil {
				return nil, err
			}
			node.Children[i] = child
		}
		return node, nil
	}

	root, err := loadNode()
	if err != nil {
		return err
	}

	bt.root = root
	return nil
}
