package apiservice

// GoAPIResponseModel reports go-api's response_model flag for a "METHOD pattern" key and whether the table names it.
func GoAPIResponseModel(key string) (model, named bool) {
	model, named = responseModelRoutes[key]
	return model, named
}
