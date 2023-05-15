// set (mango.utils.set) provides an idiomatic, simple set datastructure for Go.
package set

// an empty struct uses no memory
type void struct{}

type Setter[K comparable] interface {
	Add(...K)
	Remove(...K)
	Contains(...K) bool
	Len() int
	ToSlice() []K
	Union(...Set[K]) Set[K]
	Intersection(...Set[K]) Set[K]
}

type Set[K comparable] map[K]void

// New instantiates a New empty Set
func New[K comparable](elems ...K) Set[K] {
	ret := make(Set[K])
	ret.Add(elems...)
	return ret
}

// NewCharSet instantiates a New set of type rune
// from a string
func NewCharSet(s string) Set[rune] {
	return New([]rune(s)...)
}

// Add adds one or more elements to the Set
func (s Set[K]) Add(elems ...K) {
	for _, elem := range elems {
		s[elem] = void{}
	}
}

// Remove removes one or more elements from the Set
func (s Set[K]) Remove(elems ...K) {
	for _, elem := range elems {
		delete(s, elem)
	}
}

// Contains returns true if the Set contains all of the elements
func (s Set[K]) Contains(elems ...K) bool {
	for _, elem := range elems {
		if _, ok := s[elem]; !ok {
			return false
		}
	}
	return true
}

// Len returns the number of elements in the Set
func (s Set[K]) Len() int {
	return len(s)
}

// ToSlice returns a slice of all elements in the Set
func (s Set[K]) ToSlice() []K {
	ret := make([]K, 0, len(s))
	for elem := range s {
		ret = append(ret, elem)
	}
	return ret
}

// Union returns a new Set with all elements from all Sets
func (s Set[K]) Union(sets ...Set[K]) Set[K] {
	ret := New[K]()
	for elem := range s {
		ret.Add(elem)
	}
	for _, set := range sets {
		for elem := range set {
			ret.Add(elem)
		}
	}
	return ret
}

// Intersection returns a new Set with all elements that are in all Sets
func (s Set[K]) Intersection(sets ...Set[K]) Set[K] {
	ret := New[K]()
	for elem := range s {
		ret.Add(elem)
	}
	for _, set := range sets {
		for elem := range ret {
			if _, ok := set[elem]; !ok {
				delete(ret, elem)
			}
		}
	}
	return ret
}
