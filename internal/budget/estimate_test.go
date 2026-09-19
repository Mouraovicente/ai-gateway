package budget

import "testing"

func TestEstimateTokens_UsesPromptBytesAndMaxTokens(t *testing.T) {
	got := EstimateTokens(400, 200)
	want := 400/4 + 200
	if got != want {
		t.Fatalf("EstimateTokens(400, 200) = %d, want %d", got, want)
	}
}

func TestEstimateTokens_DefaultsMaxTokensTo1024WhenAbsent(t *testing.T) {
	got := EstimateTokens(400, 0)
	want := 400/4 + 1024
	if got != want {
		t.Fatalf("EstimateTokens(400, 0) = %d, want %d", got, want)
	}
}
