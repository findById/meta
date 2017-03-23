package store

type MetaStore struct {
	producerId string
	consumerId string
	messageId  uint16
	topic      []byte
	payload    []byte
}