package btree

import (
	"fmt"
	"strings"
)

type BTreeNode[K any, V any] struct {
	isLeaf   bool
	keys     []K
	children []*BTreeNode[K, V]
	values   []V
}

type BTree[K any, V any] struct {
	root    *BTreeNode[K, V]
	t       int // Minimum degree
	compare func(a, b K) int
}

func NewBTree[K any, V any](t int, compare func(a, b K) int) *BTree[K, V] {
	return &BTree[K, V]{t: t, root: nil, compare: compare}
}

func (bt *BTree[K, V]) Insert(key K, value V) {
	if bt.root == nil {
		bt.root = &BTreeNode[K, V]{isLeaf: true}
		bt.root.keys = []K{key}
		bt.root.values = []V{value}
		return
	}

	if len(bt.root.keys) == (2*bt.t)-1 {
		newRoot := &BTreeNode[K, V]{isLeaf: false}
		newRoot.children = append(newRoot.children, bt.root)
		bt.splitChild(newRoot, 0)
		bt.root = newRoot
	}

	bt.insertNonFull(bt.root, key, value, bt.compare)
}

func (bt *BTree[K, V]) insertNonFull(node *BTreeNode[K, V], key K, value V, compare func(a, b K) int) {
	i := len(node.keys) - 1

	if node.isLeaf {
		node.keys = append(node.keys, key)
		node.values = append(node.values, value)
		for i >= 0 && compare(key, node.keys[i]) < 0 {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		node.keys[i+1] = key
		node.values[i+1] = value
	} else {
		for i >= 0 && compare(key, node.keys[i]) < 0 {
			i--
		}
		i++
		if len(node.children[i].keys) == (2*bt.t)-1 {
			bt.splitChild(node, i)
			if compare(key, node.keys[i]) > 0 {
				i++
			}
		}
		bt.insertNonFull(node.children[i], key, value, compare)
	}
}

func (bt *BTree[K, V]) splitChild(parent *BTreeNode[K, V], i int) {
	t := bt.t
	child := parent.children[i]
	newChild := &BTreeNode[K, V]{isLeaf: child.isLeaf}
	parent.keys = append(parent.keys[:i], append([]K{child.keys[t-1]}, parent.keys[i:]...)...)
	parent.values = append(parent.values[:i], append([]V{child.values[t-1]}, parent.values[i:]...)...)
	parent.children = append(parent.children[:i+1], append([]*BTreeNode[K, V]{newChild}, parent.children[i+1:]...)...)

	newChild.keys = make([]K, t-1)
	newChild.values = make([]V, t-1)

	copy(newChild.keys, child.keys[t:])
	copy(newChild.values, child.values[t:])
	child.keys = child.keys[:t-1]
	child.values = child.values[:t-1]

	if !child.isLeaf {
		newChild.children = make([]*BTreeNode[K, V], t)
		copy(newChild.children, child.children[t:])
		child.children = child.children[:t]
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
	for i < len(node.keys) && bt.compare(key, node.keys[i]) > 0 {
		i++
	}

	if i < len(node.keys) && bt.compare(key, node.keys[i]) == 0 {
		return node.values[i], true // Key found, return the associated value
	}

	if node.isLeaf {
		var zero V
		return zero, false // Key not found in a leaf node
	}

	return bt.search(node.children[i], key) // Search in the appropriate child
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
	for i, key := range node.keys {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%v", key)
	}
	fmt.Println()

	if !node.isLeaf {
		for _, child := range node.children {
			bt.prettyPrint(child, level+1)
		}
	}
}
