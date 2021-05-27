package models

type DbRequest struct {
	Value string `json:"value"`
}

type DbResponse struct {
	Key string `json:"key"`
	Value string `json:"value"`
}
