package memtable

import (
	"bytes"
)

type avlTree struct {
	root *Node
}

type Node struct {
	key    []byte
	value  []byte
	height int
	left   *Node
	right  *Node
}

func (tree *avlTree) put(key []byte, value []byte) error {
	newNode := Node{
		key,
		value,
		1,
		nil,
		nil,
	}
	if tree.root == nil {
		tree.root = &newNode
		return nil
	}
	tree.root = insert(tree.root, &newNode)
	return nil
}

func insert(n *Node, nNode *Node) *Node {
	if n == nil {
		return nNode
	}
	if bytes.Compare(nNode.key, n.key) < 0 { // if new < current
		n.left = insert(n.left, nNode)
	} else if bytes.Compare(nNode.key, n.key) > 0 { // if new > current
		n.right = insert(n.right, nNode)
	} else { // node exists, replace value
		n.value = nNode.value
		return n
	}

	return balance(n)
}

func height(n *Node) int {
	if n == nil {
		return 0
	}
	return n.height
}

func updateHeight(n *Node) {
	if n != nil {
		n.height = 1 + max(height(n.left), height(n.right))
	}
}

func balance(n *Node) *Node {
	if n == nil {
		return n
	}
	if (height(n.left) - height(n.right)) > 1 { // left heavy
		if height(n.left.left) >= height(n.left.right) {
			n = rotateRight(n)
		} else {
			n = rotateLeftRight(n)
		}
	} else if height(n.right)-height(n.left) > 1 { // right heavy
		if height(n.right.right) >= height(n.right.left) {
			n = rotateLeft(n)
		} else {
			n = rotateRightLeft(n)
		}
	}
	updateHeight(n)
	return n
}

func rotateRight(y *Node) *Node {
	x := y.left
	T2 := x.right

	x.right = y
	y.left = T2

	updateHeight(y)
	updateHeight(x)

	return x
}

func rotateLeft(x *Node) *Node {
	y := x.right
	T2 := y.left

	y.left = x
	x.right = T2

	updateHeight(x)
	updateHeight(y)

	return y
}

func rotateLeftRight(n *Node) *Node {
	n.left = rotateLeft(n.left)
	return rotateRight(n)
}

func rotateRightLeft(n *Node) *Node {
	n.right = rotateRight(n.right)
	return rotateLeft(n)
}
