package paths

import (
	"path"
)

func Join(elems ...string) string {

	var output string
	for _, e := range elems {
		cleanedPathElement := path.Join("/", path.Clean(e))
		output = path.Join(output, cleanedPathElement)
	}

	// If the path didnt start with a /, remove the added slash
	if len(elems) > 0 && len(output) > 0 {

		if len(elems[0]) > 0 {
			if elems[0][0] != '/' {
				return output[1:]
			}
		}

		if elems[0] == "" {
			return output[1:]
		}

	}

	return output
}
