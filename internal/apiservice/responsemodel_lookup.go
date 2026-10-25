package apiservice

// GoAPIResponseModel reports go-api's response_model flag for a "METHOD
// pattern" route key, and whether the table names the route at all.
func GoAPIResponseModel(key string) (model, named bool) {
	model, named = responseModelRoutes[key]
	return model, named
}
