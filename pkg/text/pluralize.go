package text

func Pluralize(word string, n int) string {
	if n != 1 {
		return word + `s`
	} else {
		return word
	}
}
