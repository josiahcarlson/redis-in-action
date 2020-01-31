package repository

import "time"

type Inventory struct {
	Id     string
	Data   string
	Cached int64
}

func NewInventory(id, data string, cached int64) Inventory {
	return Inventory{
		Id:     id,
		Data:   data,
		Cached: cached,
	}
}

func Get(id string) Inventory {
	return NewInventory(id, "data to cache...", time.Now().Unix())
}
