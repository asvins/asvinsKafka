package common_io

import "errors"

/*
*	Common events
 */
const (
	EVENT_CREATED = iota
	EVENT_UPDATED
	EVENT_DELETED
)

/*
*	BuildTopicFromCommonEvent builds the topic string for the common event:
*		EVENT_CREATED, EVENT_UPDATED, EVENT_DELETED
 */
func BuildTopicFromCommonEvent(event int, prefix string) (string, error) {
	var sufix string

	switch event {
	case EVENT_CREATED:
		sufix = "_created"
	case EVENT_UPDATED:
		sufix = "_updated"
	case EVENT_DELETED:
		sufix = "_deleted"
	default:
		return "", errors.New("[ERROR] Event not found")
	}

	return prefix + sufix, nil
}
