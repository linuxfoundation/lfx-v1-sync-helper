// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Package main synchronizes LFX v1 and v2 resources.
package main

import (
	"context"
	"errors"
	"fmt"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// Recovery adopts only the original request; it must not silently lose edits
// ACKed while the reverse mapping was absent. Compare current canonical fields,
// not the stale event, and leave drift for an explicit source-of-truth decision.
func validateCommitteeCreatePayload(ctx context.Context, uid, projectSFID, expected string) error {
	base, _, err := fetchCommitteeBase(ctx, uid)
	if err != nil {
		var missing *committeeservice.NotFoundError
		if errors.As(err, &missing) {
			return fmt.Errorf("%w: committee disappeared during payload validation", errCommitteeCreateManual)
		}
		return err
	}
	if base == nil || base.UID == nil || *base.UID != uid || base.ProjectUID == nil {
		return fmt.Errorf("%w: invalid current committee payload", errCommitteeCreateManual)
	}
	project, err := getMappingEntryWithRetry(ctx, "project.uid."+*base.ProjectUID)
	if err != nil {
		return err
	}
	if string(project.Value) != projectSFID {
		return fmt.Errorf("%w: current committee project changed", errCommitteeCreateManual)
	}
	text := func(value *string) string {
		if value == nil {
			return ""
		}
		return *value
	}
	payload := projectServiceCommitteeCreate{
		Name: text(base.Name), Category: mapV2CategoryToV1(text(base.Category)), Description: text(base.Description),
		Website: text(base.Website), MailingList: text(base.MailingList), ChatChannel: text(base.ChatChannel),
		SSOGroupName: text(base.SsoGroupName), JoinMode: base.JoinMode, PublicName: text(base.DisplayName),
		EnableVoting: &base.EnableVoting, PublicEnabled: &base.Public, SSOGroupEnabled: &base.SsoGroupEnabled,
	}
	parent, err := resolveV2CommitteeParentSFID(ctx, projectSFID, map[string]any{"parent_uid": text(base.ParentUID)}, false)
	if err != nil {
		return err
	}
	if parent != nil && *parent != "NONE" {
		payload.CommitteeID = *parent
	}
	settings, _, err := fetchCommitteeSettings(ctx, uid)
	if err != nil {
		var missing *committeeservice.NotFoundError
		if !errors.As(err, &missing) {
			return err
		}
	} else if settings == nil {
		return errors.New("empty current committee settings payload")
	} else {
		payload.BusinessEmailRequired = &settings.BusinessEmailRequired
	}
	if committeeCreateFingerprint(payload) != expected {
		return fmt.Errorf("%w: current committee fields drifted from the recorded create request", errCommitteeCreateManual)
	}
	return nil
}
