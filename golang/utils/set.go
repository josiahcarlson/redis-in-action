package utils

import "sort"

type Set map[string]struct{}

func (s *Set) Intersection(items *[]string) []string {
	res := make([]string, 0, len(*s))
	keys := s.Getkeys()
	sort.Slice(*items, func(i, j int) bool {
		return (*items)[i] < (*items)[j]
	})

	for _, k := range keys {
		idx := sort.SearchStrings(*items, k)
		if k != (*items)[idx] {
			res = append(res, k)
		}
	}
	return res
}

func (s *Set) Getkeys() []string {
	keys := make([]string, 0, len(*s))
	for k := range *s {
		keys = append(keys, k)
	}
	return keys
}

func (s *Set) Add(match string) {
	if _, ok := (*s)[match]; !ok {
		(*s)[match] = struct{}{}
	}
}
