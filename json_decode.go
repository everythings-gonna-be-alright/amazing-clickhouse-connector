package main

import (
	"encoding/json"
	"reflect"
)

func createMapFromJSON(jsonData string) (interface{}, error) {
	// Unmarshal the JSON into a map
	var data map[string]interface{}
	err := json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return nil, err
	}

	// Create a new map using reflection
	newMap := reflect.MakeMap(reflect.TypeOf(data))

	// Loop over the key-value pairs in the JSON data and add them to the new map
	for key, value := range data {
		newMap.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
	}

	// Return the new map
	return newMap.Interface(), nil
}
