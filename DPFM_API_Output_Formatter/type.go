package dpfm_api_output_formatter

type Country struct {
	Country      string      `json:"Country"`
	GlobalRegion string      `json:"GlobalRegion"`
	CountryText  CountryText `json:"CountryText"`
}

type CountryText struct {
	Country     string `json:"Country"`
	Language    string `json:"Language"`
	CountryName string `json:"CountryName"`
}
