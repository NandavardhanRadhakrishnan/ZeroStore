package bplustree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
)

type node[K any, V any] struct {
	keys     []K
	children []*node[K, V]
	values   []V
	leaf     bool
	next     *node[K, V]
}

type BPlusTree[K any, V any] struct {
	root      *node[K, V]
	compare   func(K, K) int
	maxDegree int
	minDegree int
}

func NewBPlusTree[K any, V any](degree int, compare func(K, K) int) *BPlusTree[K, V] {
	if degree < 3 {
		degree = 3 // Minimum allowed degree
	}
	return &BPlusTree[K, V]{
		root:      &node[K, V]{leaf: true, keys: make([]K, 0), values: make([]V, 0)},
		compare:   compare,
		maxDegree: degree,
		minDegree: (degree + 1) / 2,
	}
}

func (t *BPlusTree[K, V]) Insert(key K, value V) {
	fmt.Printf("Inserting key: %v, value: %v\n", key, value)
	if t.root == nil {
		t.root = &node[K, V]{leaf: true, keys: make([]K, 0), values: make([]V, 0)}
	}
	if t.root.insert(key, value, t.compare, t.maxDegree) {
		fmt.Println("Root split, creating new root")
		oldRoot := t.root
		t.root = &node[K, V]{leaf: false, keys: make([]K, 0), children: make([]*node[K, V], 0)}
		t.root.keys = append(t.root.keys, oldRoot.keys[0])
		t.root.children = append(t.root.children, oldRoot)
		if oldRoot.next != nil {
			t.root.children = append(t.root.children, oldRoot.next)
			t.root.keys = append(t.root.keys, oldRoot.next.keys[0])
		}
	}
	fmt.Printf("After insertion, root: %+v\n", t.root)
}

func (n *node[K, V]) insert(key K, value V, compare func(K, K) int, maxDegree int) bool {
	fmt.Printf("Inserting into node: %+v\n", n)
	if n.leaf {
		return n.insertIntoLeaf(key, value, compare, maxDegree)
	}
	return n.insertIntoInternal(key, value, compare, maxDegree)
}

func (n *node[K, V]) insertIntoLeaf(key K, value V, compare func(K, K) int, maxDegree int) bool {
	insertionIndex := n.findInsertionIndex(key, compare)
	fmt.Printf("Inserting into leaf at index: %d\n", insertionIndex)
	n.keys = append(n.keys, key)
	n.values = append(n.values, value)
	copy(n.keys[insertionIndex+1:], n.keys[insertionIndex:])
	copy(n.values[insertionIndex+1:], n.values[insertionIndex:])
	n.keys[insertionIndex] = key
	n.values[insertionIndex] = value

	fmt.Printf("After insertion into leaf: %+v\n", n)

	if len(n.keys) > maxDegree-1 {
		fmt.Println("Leaf node overfull, splitting")
		return n.split(maxDegree)
	}
	return false
}

func (n *node[K, V]) insertIntoInternal(key K, value V, compare func(K, K) int, maxDegree int) bool {
	insertionIndex := n.findInsertionIndex(key, compare)
	fmt.Printf("Inserting into internal node, child index: %d\n", insertionIndex)
	if n.children[insertionIndex].insert(key, value, compare, maxDegree) {
		fmt.Println("Child split, handling in internal node")
		return n.handleChildSplit(insertionIndex, maxDegree)
	}
	return false
}

func (n *node[K, V]) findInsertionIndex(key K, compare func(K, K) int) int {
	for i, k := range n.keys {
		if compare(key, k) < 0 {
			return i
		}
	}
	return len(n.keys)
}

func (n *node[K, V]) split(maxDegree int) bool {
	midIndex := len(n.keys) / 2
	newNode := &node[K, V]{leaf: n.leaf}
	newNode.keys = append(newNode.keys, n.keys[midIndex:]...)
	n.keys = n.keys[:midIndex]

	if n.leaf {
		newNode.values = append(newNode.values, n.values[midIndex:]...)
		n.values = n.values[:midIndex]
		newNode.next = n.next
		n.next = newNode
	} else {
		newNode.children = append(newNode.children, n.children[midIndex:]...)
		n.children = n.children[:midIndex+1]
	}

	return true
}

func (n *node[K, V]) handleChildSplit(index int, maxDegree int) bool {
	child := n.children[index]
	newChild := child.next
	n.keys = append(n.keys, child.keys[len(child.keys)-1])
	copy(n.keys[index+1:], n.keys[index:])
	n.keys[index] = newChild.keys[0]
	n.children = append(n.children, newChild)
	copy(n.children[index+2:], n.children[index+1:])
	n.children[index+1] = newChild

	if len(n.keys) > maxDegree-1 {
		return n.split(maxDegree)
	}
	return false
}

func (t *BPlusTree[K, V]) Search(key K) (V, bool) {
	fmt.Printf("Searching for key: %v\n", key)
	if t.root == nil {
		fmt.Println("Tree is empty")
		var zero V
		return zero, false
	}
	return t.root.search(key, t.compare)
}

func (n *node[K, V]) search(key K, compare func(K, K) int) (V, bool) {
	fmt.Printf("Searching in node: %+v\n", n)
	if n == nil {
		fmt.Println("Node is nil")
		var zero V
		return zero, false
	}

	if n.leaf {
		fmt.Println("Searching in leaf node")
		for i, k := range n.keys {
			if compare(key, k) == 0 {
				fmt.Printf("Key found at index %d\n", i)
				return n.values[i], true
			}
		}
		fmt.Println("Key not found in leaf node")
		var zero V
		return zero, false
	}

	fmt.Println("Searching in internal node")
	for i, k := range n.keys {
		if compare(key, k) <= 0 {
			fmt.Printf("Moving to child at index %d\n", i)
			return n.children[i].search(key, compare)
		}
	}
	fmt.Printf("Moving to last child at index %d\n", len(n.keys))
	return n.children[len(n.keys)].search(key, compare)
}

func (t *BPlusTree[K, V]) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(t.maxDegree)
	if err != nil {
		return nil, err
	}
	err = enc.Encode(t.root)
	return buf.Bytes(), err
}

func (t *BPlusTree[K, V]) Deserialize(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&t.maxDegree)
	if err != nil {
		return err
	}
	t.minDegree = (t.maxDegree + 1) / 2
	return dec.Decode(&t.root)
}

func (t *BPlusTree[K, V]) PrettyPrint() string {
	if t.root == nil {
		return "Empty tree"
	}
	return t.root.prettyPrint(0, "")
}

func (n *node[K, V]) prettyPrint(level int, prefix string) string {
	if n == nil {
		return prefix + "nil\n"
	}

	var result strings.Builder
	indent := strings.Repeat("  ", level)
	result.WriteString(fmt.Sprintf("%s%sNode: ", indent, prefix))

	if n.leaf {
		result.WriteString("Leaf [")
		for i, key := range n.keys {
			if i > 0 {
				result.WriteString(", ")
			}
			if i < len(n.values) {
				result.WriteString(fmt.Sprintf("%v:%v", key, n.values[i]))
			} else {
				result.WriteString(fmt.Sprintf("%v:?", key))
			}
		}
		result.WriteString("]")
		if n.next != nil {
			result.WriteString(" -> Next Leaf")
		}
		result.WriteString("\n")
	} else {
		result.WriteString("Internal [")
		for i, key := range n.keys {
			if i > 0 {
				result.WriteString(", ")
			}
			result.WriteString(fmt.Sprintf("%v", key))
		}
		result.WriteString("]\n")

		for i, child := range n.children {
			childPrefix := "├─ "
			if i == len(n.children)-1 {
				childPrefix = "└─ "
			}
			result.WriteString(child.prettyPrint(level+1, childPrefix))
		}
	}

	return result.String()
}
