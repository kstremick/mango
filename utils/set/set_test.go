package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestSet(t *testing.T) {
	s := New(2)

	if s.Len() != 1 {
		t.Errorf("Expected length of 0, got %d", s.Len())
	}

	s.Add(1, 2, 3)
	if s.Len() != 3 {
		t.Fatalf("Expected length of 3, got %d", s.Len())
	}

	if s.Contains(4) {
		t.Fatalf("Expected set to not contain 4")
	}

	s.Remove(2)
	if s.Contains(2) {
		t.Fatalf("Expected set to not contain 2")
	}

	slice := s.ToSlice()
	expectedSlice := []int{1, 3}
	if len(slice) != len(expectedSlice) {
		t.Fatalf("Expected slice length of %d, got %d", len(expectedSlice), len(slice))
	}
	sort.Ints(slice)
	sort.Ints(expectedSlice)

	if !reflect.DeepEqual(slice, expectedSlice) {
		t.Fatalf("Expected slice %v, got %v", expectedSlice, slice)
	}

	s2 := New(3, 4, 5)
	union := s.Union(s2)
	expectedUnion := New(1, 3, 4, 5)
	if !reflect.DeepEqual(union, expectedUnion) {
		t.Fatalf("Expected union %v, got %v", expectedUnion, union)
	}

	intersection := s.Intersection(s2)
	expectedIntersection := New(3)
	if !reflect.DeepEqual(intersection, expectedIntersection) {
		t.Fatalf("Expected intersection %v, got %v", expectedIntersection, intersection)
	}
}
