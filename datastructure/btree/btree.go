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
