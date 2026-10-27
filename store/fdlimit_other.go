//go:build !unix

package store

func fdBudget() int { return 256 }
