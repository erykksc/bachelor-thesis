package main

import "strings"

// Fromat list of strings to be acceptable for UNNEST argument
func joinAndQuoteStrings(list []string) string {
	var builder strings.Builder
	for i, s := range list {
		if i != 0 {
			builder.WriteRune(',')
		}
		builder.WriteRune('\'')
		builder.WriteString(s)
		builder.WriteRune('\'')
	}
	return builder.String()
}
