package index

import "testing"

func TestReturnsTrueGivenKeyValuePairsAreEqual(t *testing.T) {
	firstKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Storage"),
	}

	secondKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Storage"),
	}

	if !firstKeyValuePair.Equals(secondKeyValuePair) {
		t.Fatalf("Expected key value pairs to be equals")
	}
}

func TestReturnsFalseGivenKeyValuePairsAreNotEqualByKey(t *testing.T) {
	firstKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Storage"),
	}

	secondKeyValuePair := KeyValuePair{
		key:   []byte("B"),
		value: []byte("Storage"),
	}

	if firstKeyValuePair.Equals(secondKeyValuePair) {
		t.Fatalf("Expected key value pairs to not be equal")
	}
}

func TestReturnsFalseGivenKeyValuePairsAreNotEqualByValue(t *testing.T) {
	firstKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Storage"),
	}

	secondKeyValuePair := KeyValuePair{
		key:   []byte("A"),
		value: []byte("Database"),
	}

	if firstKeyValuePair.Equals(secondKeyValuePair) {
		t.Fatalf("Expected key value pairs to not be equal")
	}
}
