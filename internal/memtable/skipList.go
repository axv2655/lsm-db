package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
)

type skipListNode struct {
	key   []byte
	value []byte

	forward []*skipListNode
}

type skipList struct {
	maxLevel int
	level    int

	p float32

	head *skipListNode
}

type KVEntry struct {
	Key   []byte
	Value []byte
}

func newSkipList(p float32, maxLevel int) *skipList {
	return &skipList{
		maxLevel,
		0,
		p,
		&skipListNode{
			forward: make([]*skipListNode, maxLevel),
		},
	}
}

func (list *skipList) insert(key []byte, value []byte) error {
	curr := list.head
	update := make([]*skipListNode, list.maxLevel)
	for i := list.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, key) < 0 { // curr's forward < key
			curr = curr.forward[i]
		}
		update[i] = curr

	}
	curr = curr.forward[0]

	if curr != nil && bytes.Equal(curr.key, key) {
		curr.value = value
		return nil
	}

	newLevel := randomLevel(list.p, list.maxLevel)
	if newLevel > list.level {
		for i := list.level; i < newLevel; i++ {
			update[i] = list.head
		}
		list.level = newLevel
	}

	newNode := &skipListNode{
		key:     key,
		value:   value,
		forward: make([]*skipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	return nil
}

func randomLevel(p float32, maxLevel int) int {
	level := 1

	for rand.Float32() < p && level < maxLevel {
		level += 1
	}
	return level
}

func (list *skipList) get(key []byte) (*skipListNode, error) {
	curr := list.head
	for i := list.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && bytes.Compare(curr.forward[i].key, key) < 0 { // curr's forward < key
			curr = curr.forward[i]
		}
	}
	curr = curr.forward[0]

	if curr != nil && bytes.Equal(curr.key, key) && len(curr.value) != 0 {
		return curr, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (list *skipList) delete(key []byte) error {
	if _, err := list.get(key); err != nil {
		return fmt.Errorf("key does not exist")
	}
	if err := list.insert(key, []byte{}); err != nil {
		return fmt.Errorf("error: %w", err)
	}
	return nil
}

func (list *skipList) GetAll() []KVEntry {
	curr := list.head
	var entries []KVEntry
	for curr != nil {
		entry := KVEntry{
			curr.key,
			curr.value,
		}
		entries = append(entries, entry)
		curr = curr.forward[0]
	}
	return entries
}
