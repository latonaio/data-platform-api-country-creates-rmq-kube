package dpfm_api_input_reader

import (
	"data-platform-api-country-creates-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToCountry() *requests.Country {
	data := sdc.Country
	return &requests.Country{
		Country:      data.Country,
		GlobalRegion: data.GlobalRegion,
	}
}

func (sdc *SDC) ConvertToCountryText() *requests.CountryText {
	dataCountry := sdc.Country
	data := sdc.Country.CountryText
	return &requests.CountryText{
		Country:     dataCountry.Country,
		Language:    data.Language,
		CountryName: data.CountryName,
	}
}
