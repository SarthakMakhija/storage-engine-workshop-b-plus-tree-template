package index

import (
	"b+tree/index/schema"
	"bytes"
)

type KeyValuePair struct {
	key   []byte
	value []byte
}

func (keyValuePair KeyValuePair) Equals(other KeyValuePair) bool {
	if bytes.Equal(keyValuePair.value, other.value) && bytes.Equal(keyValuePair.key, other.key) {
		return true
	}
	return false
}

func (keyValuePair KeyValuePair) PrettyValue() string {
	return string(keyValuePair.value)
}

func (keyValuePair KeyValuePair) RawValue() []byte {
	return keyValuePair.value
}

func (keyValuePair KeyValuePair) PrettyKey() string {
	return string(keyValuePair.key)
}

func (keyValuePair KeyValuePair) String() string {
	return " [" + keyValuePair.PrettyKey() + " - " + keyValuePair.PrettyValue() + "] "
}

func (keyValuePair KeyValuePair) isEmpty() bool {
	if len(keyValuePair.key) == 0 && len(keyValuePair.value) == 0 {
		return true
	}
	return false
}

func (keyValuePair KeyValuePair) toPersistentKeyValuePair() schema.PersistentKeyValuePair {
	return schema.PersistentKeyValuePair{
		Key:   keyValuePair.key,
		Value: keyValuePair.value,
	}
}
