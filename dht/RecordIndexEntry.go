package dht

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/SharefulNetworks/shareful-dht/types"
)

type RecordIndexEntry struct {
	Source                  string       `json:"source"`
	Target                  string       `json:"target"`
	Meta                    []byte       `json:"meta"`
	UpdatedUnix             int64        `json:"updated_unix"`
	CreatedUnix             int64        `json:"created_unix"`
	Publisher               types.NodeID `json:"publisher"`
	PublisherAddr           string       `json:"publisher_addr"`
	TTL                     int64        `json:"ttl"`
	LocalRefreshCount       uint64       `json:"local_refresh_count"`        //record level refresh count doesn't make sense for IndexEntry as each entry in the record will be maintained by different nodes, each of which will have their own respective refresh intervals.
	EnableIndexUpdateEvents bool         `json:"enable_index_update_events"` //where true, any updates to the record will trigger the production of a SyncIndexUpdatedEvent which the parent applicaton can use to be promptly notified of any changes and undertake any application level operations, as necessary.

}

// create setter functions,
//NB: The setters are not used but MUST remain in place to allow RecordIndexEntryLike to
//    satisfy this interface and have access to the data these functions provide.

func (entry *RecordIndexEntry) SetSource(source string) {
	entry.Source = source
}

func (entry *RecordIndexEntry) SetTarget(target string) {
	entry.Target = target
}

func (entry *RecordIndexEntry) SetMeta(meta []byte) {
	entry.Meta = meta
}

func (entry *RecordIndexEntry) SetUpdatedUnix(updatedUnix int64) {
	entry.UpdatedUnix = updatedUnix
}

func (entry *RecordIndexEntry) SetCreatedUnix(createdUnix int64) {
	entry.CreatedUnix = createdUnix
}

func (entry *RecordIndexEntry) SetPublisher(publisher types.NodeID) {
	entry.Publisher = publisher
}

func (entry *RecordIndexEntry) SetPublisherAddr(publisherAddr string) {
	entry.PublisherAddr = publisherAddr
}

func (entry *RecordIndexEntry) SetTTL(ttl int64) {
	entry.TTL = ttl
}

func (entry *RecordIndexEntry) SetLocalRefreshCount(localRefreshCount uint64) {
	entry.LocalRefreshCount = localRefreshCount
}

func (entry *RecordIndexEntry) SetEnableIndexUpdateEvents(enableIndexUpdateEvents bool) {
	entry.EnableIndexUpdateEvents = enableIndexUpdateEvents
}

// AppendTargetValue - Appends a string to the existing set of Target values of the RecordIndexEntry.
// internally the values are maintained as a JSON encoded array of strings.
//
// To ensure backwards compatibility and enable a single string value or an array values to be used,
// where target has ALREADY been assigned a string value at the time of the the call, the value is
// appended to the new array of string first before the target value is appended, the full
// array is then JSON encoded string and set as the new target value of the RecordIndexEntry.
// Therefore, the RecordIndexEntry can be used with a single string value or an array of
// string values, and the AppendTargetValue method can be used to append new values to the existing
// target value, regardless of whether it is a single string or an array of strings.
func (entry *RecordIndexEntry) AppendTargetValue(newTargetValue string) error {

	blendedTargetArr := make([]string, 0)

	if entry.targetIsAnArray() {

		//where we already have an array unmarshal it into the blended array..
		unmarshalErr := json.Unmarshal([]byte(entry.Target), &blendedTargetArr)
		if unmarshalErr != nil {
			return unmarshalErr
		}

	} else {

		//where we hace a single string append it to our blended array.
		if len(entry.Target) > 0 {
			blendedTargetArr = append(blendedTargetArr, entry.Target)
		}
	}

	//in nay case append the newly provided value to our blended array.
	blendedTargetArr = append(blendedTargetArr, newTargetValue)

	//dedupe our blended array in case any duplicates have been introduced as a result of this append operation, all target values MUST be unique.
	blendedTargetArr = entry.dedupeValues(blendedTargetArr)

	//marshal our blended array back into a JSON string and set it as the new target value of the RecordIndexEntry.
	marshaledBlendedTargetArr, marshalErr := json.Marshal(blendedTargetArr)
	if marshalErr != nil {
		return marshalErr
	}

	entry.Target = string(marshaledBlendedTargetArr)
	entry.UpdatedUnix = time.Now().UnixMilli()
	return nil
}

// RemoveTargetValue - Removes a string target value from the existing set of Target values
// of the RecordIndexEntry. Where the target value to be removed does not exist within
// the existing set of target values, the RecordIndexEntry is left unchanged and an error is returned.
func (entry *RecordIndexEntry) RemoveTargetValue(targetValueToRemove string) error {

	if len(entry.Target) < 1 {
		return fmt.Errorf("cannot remove target value from RecordIndexEntry where target value is empty")
	}

	blendedTargetArr := make([]string, 0)

	//parse the existing target value(s) to an array.
	if entry.targetIsAnArray() {
		unmarshalErr := json.Unmarshal([]byte(entry.Target), &blendedTargetArr)
		if unmarshalErr != nil {
			return unmarshalErr
		}
	} else {
		//where we have a single string append it to our blended array.
		if len(entry.Target) > 0 {
			blendedTargetArr = append(blendedTargetArr, entry.Target)
		}

	}

	//check if the provided target value to remove exists within our blended array,
	// if not return an error.
	if !slices.Contains(blendedTargetArr,targetValueToRemove){
		return fmt.Errorf("cannot remove target value '%s' from RecordIndexEntry where it does not exist within the existing set of target values", targetValueToRemove)
	}

	//if it does exist, remove it from the blended array.
	blendedTargetArr = slices.Delete(
		blendedTargetArr, 
		slices.Index(blendedTargetArr, targetValueToRemove), 
		slices.Index(blendedTargetArr, targetValueToRemove)+1,
	)

	entry.UpdatedUnix = time.Now().UnixMilli()
	//dedupe our blended array in case any duplicates have been introduced as a result of this remove operation, all target values MUST be unique.
	blendedTargetArr = entry.dedupeValues(blendedTargetArr)

	//marshal our blended array back into a JSON string and set it as the new target value of the RecordIndexEntry.
	marshaledBlendedTargetArr, marshalErr := json.Marshal(blendedTargetArr)
	if marshalErr != nil {
		return marshalErr
	}
	entry.Target = string(marshaledBlendedTargetArr)

	//return nil to indicate successful removal of the target value from the RecordIndexEntry.
	return nil
	
}

//ListTargetValues - Returns the Target values of the RecordIndexEntry as a slice of strings. 
// Where the target value is a single string, the returned slice will contain one element 
// which is that string, where the target value is an array of strings, the returned slice 
// will contain all elements of that array. Where the target value is empty, 
// an empty slice is returned.
func (entry *RecordIndexEntry) ListTargetValues() ([]string, error) {

	if len(entry.Target) < 1 {
		return []string{}, nil
	}

	blendedTargetArr := make([]string, 0)

	//parse the existing target value(s) to an array.
	if entry.targetIsAnArray() {
		unmarshalErr := json.Unmarshal([]byte(entry.Target), &blendedTargetArr)
		if unmarshalErr != nil {
			return []string{}, unmarshalErr
		}
	}else {
		//where we have a single string append it to our blended array.
		if len(entry.Target) > 0 {
			blendedTargetArr = append(blendedTargetArr, entry.Target)
		}
	}

	return blendedTargetArr, nil
}

// create getter functions
func (entry RecordIndexEntry) GetSource() string {
	return entry.Source
}

func (entry RecordIndexEntry) GetTarget() string {
	return entry.Target
}

func (entry RecordIndexEntry) GetMeta() []byte {
	return entry.Meta
}

func (entry RecordIndexEntry) GetUpdatedUnix() int64 {
	return entry.UpdatedUnix
}

func (entry RecordIndexEntry) GetCreatedUnix() int64 {
	return entry.CreatedUnix
}

func (entry RecordIndexEntry) GetPublisher() types.NodeID {
	return entry.Publisher
}

func (entry RecordIndexEntry) GetPublisherAddr() string {
	return entry.PublisherAddr
}

func (entry RecordIndexEntry) GetTTL() int64 {
	return entry.TTL
}

func (entry RecordIndexEntry) GetLocalRefreshCount() uint64 {
	return entry.LocalRefreshCount
}

func (entry RecordIndexEntry) GetEnableIndexUpdateEvents() bool {
	return entry.EnableIndexUpdateEvents
}

func (entry RecordIndexEntry) targetIsAnArray() bool {

	if len(entry.Target) > 0 && entry.Target[0] == '[' && entry.Target[len(entry.Target)-1] == ']' {
		return true
	}
	return false
}

func (entry RecordIndexEntry) dedupeValues(values []string) []string {

	uniqueValuesMap := make(map[string]bool)
	dedupedValues := make([]string, 0)

	for _, value := range values {
		uniqueValuesMap[value] = true
	}
	for value := range uniqueValuesMap {
		dedupedValues = append(dedupedValues, value)
	}
	return dedupedValues
}
