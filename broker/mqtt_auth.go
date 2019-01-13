package broker

func CheckAuthPermission(username, password []byte) bool {
	return true
}

func CheckTopicPermission(topic, method string) bool {
	return true
}
