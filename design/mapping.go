// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package design defines the HTTP contract for the legacy mapping API.
package design

//revive:disable-next-line:dot-imports Goa's standard DSL convention.
import . "goa.design/goa/v3/dsl" //nolint:staticcheck // Goa's standard DSL convention.

var _ = API("v1-mapping", func() {
	Title("LFX V1 Mapping API")
	Description("Read existing legacy project and committee IDs for V2 resources.")
})

// JWTAuth requires a Heimdall token issued after the team membership check.
var JWTAuth = JWTSecurity("jwt", func() {
	Description("Heimdall JWT issued after the mapping-access team membership check.")
})

var _ = Service("mapping", func() {
	Method("lookup", func() {
		Security(JWTAuth)
		Payload(func() {
			Token("token", String, "Heimdall bearer token")
			Attribute("resource_type", String, func() { Enum("project", "committee") })
			Attribute("uid", String, "V2 resource UUID", func() {
				Pattern(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
				Example("00000000-0000-4000-8000-000000000001")
			})
			Required("token", "resource_type", "uid")
		})
		Result(func() {
			Attribute("resource_type", String, func() {
				Enum("project", "committee")
				Example("committee")
			})
			Attribute("uid", String, "Requested V2 resource UUID", func() {
				Example("00000000-0000-4000-8000-000000000001")
			})
			Attribute("project_sfid", String, "Legacy project Salesforce ID", func() {
				Pattern(`^(?:[a-zA-Z0-9]{15}|[a-zA-Z0-9]{18})$`)
				Example("a09000000000001AAA")
			})
			Attribute("committee_sfid", String, "Legacy committee ID (a UUID); present only for committees", func() {
				Example("00000000-0000-4000-8000-000000000002")
			})
			Required("resource_type", "uid", "project_sfid")
		})
		Error("BadRequest", ErrorResult)
		Error("Unauthorized", ErrorResult)
		Error("NotFound", ErrorResult)
		Error("Gone", ErrorResult, "Mapping has a retained deletion marker")
		Error("BadGateway", ErrorResult, func() { Fault() })
		Error("ServiceUnavailable", ErrorResult, func() { Temporary() })
		Error("GatewayTimeout", ErrorResult, func() {
			Temporary()
			Timeout()
		})
		HTTP(func() {
			GET("/v1-sync-helper/mappings/{resource_type}/{uid}")
			Header("token:Authorization")
			Response(StatusOK)
			Response("BadRequest", StatusBadRequest)
			Response("Unauthorized", StatusUnauthorized)
			Response("NotFound", StatusNotFound)
			Response("Gone", StatusGone)
			Response("BadGateway", StatusBadGateway)
			Response("ServiceUnavailable", StatusServiceUnavailable)
			Response("GatewayTimeout", StatusGatewayTimeout)
		})
	})
})
