package set

import (
	"fmt"
	"iter"
	"strings"
)

type node[T comparable] struct {
	prev  *node[T]
	value T
	next  *node[T]
}

type Set[T comparable] interface {
	Add(items ...T)
	Contains(item T) bool
	Remove(items ...T)
	Union(other Set[T]) Set[T]
	Intersection(other Set[T]) Set[T]
	Difference(other Set[T]) Set[T]
	SymmetricDifference(other Set[T]) Set[T]
	Values() iter.Seq[T]
	Size() int
	String() string
}

type set[T comparable] struct {
	data map[T]*node[T]
	head *node[T]
	tail *node[T]
}

func New[T comparable](items ...T) Set[T] {
	s := &set[T]{data: make(map[T]*node[T])}
	for _, item := range items {
		s.Add(item)
	}
	return s
}

func (s *set[T]) Add(items ...T) {
	tail := s.tail
	for _, item := range items {
		if _, ok := s.data[item]; ok {
			continue
		}
		n := &node[T]{prev: tail, value: item}
		if s.head == nil {
			s.head = n
		}
		if tail != nil {
			tail.next = n
		}
		s.data[item] = n
		tail = n
	}
	s.tail = tail
}

func (s *set[T]) Contains(item T) bool {
	_, ok := s.data[item]
	return ok
}

func (s *set[T]) Remove(items ...T) {
	for _, item := range items {
		if n, ok := s.data[item]; ok {
			if n.prev != nil {
				n.prev.next = n.next
			} else {
				s.head = n.next
			}
			if n.next != nil {
				n.next.prev = n.prev
			} else {
				s.tail = n.prev
			}
			n.prev = nil
			n.next = nil
			delete(s.data, item)
		}
	}
}

func (s *set[T]) Union(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s.Values() {
		result.Add(item)
	}
	for item := range other.Values() {
		result.Add(item)
	}
	return result
}

func (s *set[T]) Intersection(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s.Values() {
		if other.Contains(item) {
			result.Add(item)
		}
	}
	return result
}

func (s *set[T]) Difference(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s.Values() {
		if !other.Contains(item) {
			result.Add(item)
		}
	}
	return result
}

func (s *set[T]) SymmetricDifference(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s.Values() {
		if !other.Contains(item) {
			result.Add(item)
		}
	}
	for item := range other.Values() {
		if !s.Contains(item) {
			result.Add(item)
		}
	}
	return result
}

func (s *set[T]) Values() iter.Seq[T] {
	cur := s.head
	return func(yield func(T) bool) {
		for cur != nil {
			if !yield(cur.value) {
				return
			}
			cur = cur.next
		}
	}
}

func (s *set[T]) Size() int {
	return len(s.data)
}

func (s *set[T]) String() string {
	if len(s.data) == 0 {
		return "{}"
	}
	str := strings.Builder{}
	str.WriteString("{")
	i := 0
	for item := range s.Values() {
		fmt.Fprintf(&str, "%v", item)
		if i < len(s.data)-1 {
			str.WriteString(", ")
		}
		i++
	}
	str.WriteByte('}')
	return str.String()
}
