package pool

import (
	"strings"
)

type Errors []error

var _ error = (*Errors)(nil)

func (es Errors) Error() string {
	strs := make([]string, es.Len())
	for i, e := range es {
		strs[i] = e.Error()
	}

	return strings.Join(strs, "; ")
}

func (es Errors) Len() int {
	return len(es)
}

func (es *Errors) Add(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		} else if errors, ok := err.(Errors); ok {
			es.Add(errors...)
		} else {
			ok = true
			for _, e := range *es {
				if e == err {
					ok = false
					break
				}
			}
			if ok {
				*es = append(*es, err)
			}
		}
	}
}
