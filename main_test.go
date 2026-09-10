package main

import "testing"

func sum(xs []int) int {
	t := 0
	for _, x := range xs {
		t += x
	}
	return t
}

func TestAllocateWorkers(t *testing.T) {
	t.Run("proportional split", func(t *testing.T) {
		got := allocateWorkers([]float64{50, 30, 20}, 10)
		want := []int{5, 3, 2}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("alloc = %v, want %v", got, want)
			}
		}
		if sum(got) != 10 {
			t.Errorf("total = %d, want 10", sum(got))
		}
	})

	t.Run("relative weights normalize", func(t *testing.T) {
		// 3:1 over 8 workers -> 6:2.
		got := allocateWorkers([]float64{3, 1}, 8)
		if got[0] != 6 || got[1] != 2 {
			t.Errorf("alloc = %v, want [6 2]", got)
		}
	})

	t.Run("every target gets at least one worker", func(t *testing.T) {
		got := allocateWorkers([]float64{1, 1, 1, 1}, 2) // fewer workers than targets
		if len(got) != 4 || sum(got) != 4 {
			t.Fatalf("alloc = %v, want four 1s", got)
		}
		for i, w := range got {
			if w < 1 {
				t.Errorf("target %d got %d workers, want >=1", i, w)
			}
		}
	})

	t.Run("remainder distributed, total preserved", func(t *testing.T) {
		got := allocateWorkers([]float64{1, 1, 1}, 10)
		if sum(got) != 10 {
			t.Errorf("total = %d, want 10 (got %v)", sum(got), got)
		}
	})

	t.Run("zero weights fall back to equal split", func(t *testing.T) {
		got := allocateWorkers([]float64{0, 0}, 4)
		if got[0] != 2 || got[1] != 2 {
			t.Errorf("alloc = %v, want [2 2]", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := allocateWorkers(nil, 10); got != nil {
			t.Errorf("alloc = %v, want nil", got)
		}
	})
}
