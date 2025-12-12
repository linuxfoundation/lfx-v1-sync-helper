// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/teambition/rrule-go"
)

const (
	occurrenceStatusAvailable = "available"
	occurrenceStatusCancel    = "cancel"
	meetingEndBuffer          = 40 * time.Minute
)

var weekdaysABBRV = []string{"SU", "MO", "TU", "WE", "TH", "FR", "SA"}
var typeName = []string{"Daily", "Weekly", "Monthly"}

// calculateOccurrences generates occurrence objects for a meeting, which can optionally include past or cancelled occurrences
func calculateOccurrences(ctx context.Context, meeting meetingInput, pastOccurrences bool, includeCancelled bool, numOccurrencesToReturn int) (result []ZoomMeetingOccurrence, err error) {
	timerNow := time.Now()
	// Occurrences only exist for recurring meetings
	if meeting.Recurrence == nil {
		return result, nil
	}

	meetingStartTime, err := time.Parse(time.RFC3339, meeting.StartTime)
	if err != nil {
		logger.With(errKey, err, "meeting_id", meeting.UID, "start_time", meeting.StartTime).ErrorContext(ctx, "failed to parse meeting start_time")
		return nil, err
	}

	location := time.UTC
	if meeting.Timezone != "" {
		meetingStartTime, err = timeInLocation(meetingStartTime, meeting.Timezone)
		if err != nil {
			return nil, err
		}

		location = meetingStartTime.Location()
	}

	// Get all occurrences patterns to calculate occurrences
	occurrencesPattern := []ZoomMeetingOccurrence{}
	// First add the base meeting recurrence
	// Store start times as Unix string
	occurrencesPattern = append(occurrencesPattern, ZoomMeetingOccurrence{
		OccurrenceID: strconv.FormatInt(meetingStartTime.Unix(), 10),
		StartTime:    meetingStartTime.Format(time.RFC3339), // base meeting start time
		Duration:     meeting.Duration,
		Recurrence:   meeting.Recurrence,
	})
	// Then add all recurrences for occurrences with a recurrence pattern, and include
	// start time to determine when the recurrence patterns start/end.
	occurrenceStartTimeUnix := occurrencesPattern[0].OccurrenceID
	occurrenceStartTimeFmt := occurrencesPattern[0].StartTime
	for _, updatedOcc := range meeting.UpdatedOccurrences {
		if updatedOcc.Recurrence != nil {
			// Update start time to occurrence start time.
			// For use in case an occurrence doesn't have a new start time.
			if updatedOcc.NewOccurrenceID != "" {
				occurrenceStartTimeUnix = updatedOcc.NewOccurrenceID
				// Convert occurrence start time to timeRFC3339 format to make the time easier to read in the logs
				occurrenceStartTimeUnixInt, err := strconv.ParseInt(occurrenceStartTimeUnix, 10, 64)
				if err != nil {
					logger.With(errKey, err, "meeting_id", meeting.UID, "occurrence_id", occurrenceStartTimeUnix, "occurrence_start_time", occurrenceStartTimeFmt).ErrorContext(ctx, "failed to parse occurrence start_time")
					return nil, err
				}
				occurrenceStartTimeFmt = time.Unix(occurrenceStartTimeUnixInt, 0).Format(time.RFC3339)
			}
			occurrencesPattern = append(occurrencesPattern, ZoomMeetingOccurrence{
				OccurrenceID: occurrenceStartTimeUnix,
				StartTime:    occurrenceStartTimeFmt,
				Recurrence:   updatedOcc.Recurrence,
				Duration:     updatedOcc.Duration,
				Title:        updatedOcc.Title,
				Description:  updatedOcc.Description,
			})
		}
	}
	slices.SortFunc(occurrencesPattern, func(a, b ZoomMeetingOccurrence) int {
		aUnix, _ := strconv.ParseInt(a.OccurrenceID, 10, 64)
		bUnix, _ := strconv.ParseInt(b.OccurrenceID, 10, 64)
		if aUnix < bUnix {
			return -1
		} else if aUnix > bUnix {
			return 1
		}
		return 0
	})
	logger.With("meeting_id", meeting.UID, "recurrences", occurrencesPattern).DebugContext(ctx, "list of recurrence patterns")

	var allFollowing bool
	var currentStartTime = meetingStartTime
	var currentDuration int
	var previousOccurrence ZoomMeetingOccurrence
	var previousOldOccurrenceID string

	// Loop through all recurrence patterns to generate the occurrences
	// from each start time to the next recurrence pattern start time.
	for occurrencePatternIdx, occurrencePattern := range occurrencesPattern {
		logger.With("meeting_id", meeting.UID, "recurrence", occurrencePattern, "idx", occurrencePatternIdx).DebugContext(ctx, "current recurrence")
		// Determine next recurrence start time to know how long recurrence pattern lasts
		var nextRecurrenceTimeUnix int64
		if occurrencePatternIdx < len(occurrencesPattern)-1 && occurrencesPattern[occurrencePatternIdx+1].OccurrenceID != "" {
			nextRecurrenceTimeUnix, err = strconv.ParseInt(occurrencesPattern[occurrencePatternIdx+1].OccurrenceID, 10, 64)
			if err != nil {
				logger.With(errKey, err, "meeting_id", meeting.UID, "next_recurrence_occurrence_id", occurrencesPattern[occurrencePatternIdx+1].OccurrenceID, "next_recurrence_start_time", occurrencesPattern[occurrencePatternIdx+1].StartTime).ErrorContext(ctx, "failed to parse next recurrence start_time")
				return nil, err
			}
		}
		logger.With("meeting_id", meeting.UID, "current_recurrence", occurrencePattern, "next_recurrence_start_time_unix", nextRecurrenceTimeUnix, "next_recurrence_start_time", time.Unix(nextRecurrenceTimeUnix, 0).Format(time.RFC3339)).DebugContext(ctx, "next recurrence start time")

		// Skip the recurrence pattern if past occurrences are not included
		// and the next recurrence start time is before the current time, which means
		// this whole recurrence pattern must also be in the past and thus can be skipped.

		if nextRecurrenceTimeUnix != 0 && !pastOccurrences && nextRecurrenceTimeUnix < time.Now().Unix() {
			continue
		}

		// Convert unix string start time into time.Time object
		unixStartTime, err := strconv.ParseInt(occurrencePattern.OccurrenceID, 10, 64)
		if err != nil {
			logger.With(errKey, err, "meeting_id", meeting.UID, "recurrence_occurrence_id", occurrencePattern.OccurrenceID, "recurrence_start_time", occurrencePattern.StartTime).ErrorContext(ctx, "failed to parse recurrence start_time")
			return nil, err
		}
		recStartTime := time.Unix(unixStartTime, 0)
		recStartTime, err = timeInLocation(recStartTime, meeting.Timezone)
		if err != nil {
			return nil, err
		}

		// Get occurrences based on reccurrence pattern and start time
		occurrences, err := getRRuleOccurrences(recStartTime, meeting.Timezone, occurrencePattern.Recurrence, nil)
		if err != nil {
			logger.With(errKey, err, "meeting_id", meeting.UID, "start_time", recStartTime, "recurrence_rrule", occurrencePattern.Recurrence).ErrorContext(ctx, "failed to get recurrence rule")
			return nil, err
		}
		occurrencesInLog := occurrences
		// only show the first 100 occurrences to avoid spamming the logs
		if len(occurrencesInLog) > 100 {
			occurrencesInLog = occurrencesInLog[:100]
		}
		logger.With("meeting_id", meeting.UID, "start_time", recStartTime, "recurrence_rrule", occurrencePattern.Recurrence, "occurrences", occurrencesInLog).DebugContext(ctx, "rrule calculated occurrences")

		currentStartTime = recStartTime
		currentDuration = occurrencePattern.Duration

		// We need to check below fields to ensure they are not empty
		currentTitle := occurrencePattern.Title
		currentDescription := occurrencePattern.Description
		if occurrencePattern.Title == "" {
			currentTitle = meeting.Title
		}
		if occurrencePattern.Description == "" {
			currentDescription = meeting.Description
		}
		for _, o := range occurrences {
			// Skip this recurrence pattern if the occurrence time
			// is after the next recurrence start time.
			if nextRecurrenceTimeUnix != 0 && nextRecurrenceTimeUnix < o.Unix() {
				logger.With("meeting_id", meeting.UID, "occurrence_id", o.Unix(), "occurrence_start_time", o).DebugContext(ctx, "skip recurrence pattern")
				break
			}
			occurrenceID := strconv.FormatInt(o.Unix(), 10)
			if occurrenceID == previousOccurrence.OccurrenceID {
				// Skip if occurrence is the same as the previous one.
				// This can happen if an occurrence has a recurrence pattern
				// that is the same as the meeting itself, so the occurrence shows
				// up in two recurrence patterns that we iterate through.
				continue
			}
			logger.With("meeting_id", meeting.UID, "occurrence_id", occurrenceID, "occurrence_start_time", o).DebugContext(ctx, "current occurrence (as calculated by RRULE, before adjustments)")

			// Need to check single updated occurrence first then all following updated occurrence
			// so that the latter gets applied to the following occurrences
			var isUpdated bool
			var updateOccAllFollowing UpdatedOccurrence
			var updateOccSingle UpdatedOccurrence
			for _, updatedOcc := range meeting.UpdatedOccurrences {
				if (updatedOcc.OldOccurrenceID == occurrenceID || updatedOcc.NewOccurrenceID == occurrenceID) && updatedOcc.AllFollowing {
					logger.With("meeting_id", meeting.UID, "occurrence", updatedOcc).DebugContext(ctx, "is an all following updated occurrence")
					isUpdated = true
					updateOccAllFollowing = updatedOcc
					allFollowing = true

					// Update the current duration and start time variables because an updated occurrence
					// with all the following enabled means the following occurrences need to use
					// the new start time and duration - so we must keep track of those values
					currentDuration = updatedOcc.Duration
					currentTitle = updatedOcc.Title
					currentDescription = updatedOcc.Description
					unixStartTime, err := strconv.ParseInt(updatedOcc.NewOccurrenceID, 10, 64)
					if err != nil {
						logger.With(errKey, err, "meeting_id", meeting.UID, "occurrence", updatedOcc).ErrorContext(ctx, "failed to parse updated occurrence start_time")
						return nil, err
					}
					currentStartTime = time.Unix(unixStartTime, 0).In(location)
					logger.With("meeting_id", meeting.UID, "current_start_time", currentStartTime, "occurrence", updatedOcc).DebugContext(ctx, "current start time changed")
				}
				// We need to check if the updated occurrence record's old or new occurrence ID matches the currently iterated
				// occurrence ID. But also it's possible that there was an all_following=true updated occurrence record that
				// effectively moved the [occurrenceID] and therefore in that case we also need to check if the updated occurrence
				// record's old or new occurrence matches the new occurrence ID of the all_following=true updated occurrence record.
				isUpdatedSingle := (updatedOcc.OldOccurrenceID == occurrenceID || updatedOcc.NewOccurrenceID == occurrenceID) && !updatedOcc.AllFollowing
				isUpdatedSingle = isUpdatedSingle || ((updatedOcc.OldOccurrenceID == updateOccAllFollowing.NewOccurrenceID || updatedOcc.NewOccurrenceID == updateOccAllFollowing.NewOccurrenceID) && allFollowing)
				if isUpdatedSingle {
					logger.With("meeting_id", meeting.UID, "occurrence", updatedOcc).DebugContext(ctx, "is a single updated occurrence")
					isUpdated = true
					updateOccSingle = updatedOcc
				}
			}

			if updateOccAllFollowing != (UpdatedOccurrence{}) || updateOccSingle != (UpdatedOccurrence{}) {
				var updatedOcc UpdatedOccurrence
				if updateOccSingle != (UpdatedOccurrence{}) {
					logger.With("meeting_id", meeting.UID, "occurrence", updateOccSingle).DebugContext(ctx, "single updated occurrence")
					updatedOcc = updateOccSingle
				} else {
					logger.With("meeting_id", meeting.UID, "occurrence", updateOccAllFollowing).DebugContext(ctx, "all following updated occurrence")
					updatedOcc = updateOccAllFollowing
				}

				// Skip past occurrences if no past occurrences are expected
				unixStartTime, err := strconv.ParseInt(updatedOcc.NewOccurrenceID, 10, 64)
				if err != nil {
					logger.With(errKey, err, "meeting_id", meeting.UID, "occurrence_id", o.Unix(), "updated_occ", updatedOcc).ErrorContext(ctx, "failed to parse updated occurrence start_time")
					return nil, err
				}

				// If updated occurrence does not have a duration, use meeting duration
				if updatedOcc.Duration == 0 {
					updatedOcc.Duration = meeting.Duration
				}

				if !pastOccurrences && isOccurrencePast(time.Unix(unixStartTime, 0), updatedOcc.Duration) {
					logger.With("meeting_id", meeting.UID, "occurrence_id", o.Unix(), "occurrence_start_time", o).DebugContext(ctx, "skipping updated occurrence because it is a past occurrence")
					continue
				}

				// Skip updated occurrence if previous occurrence was an updated occurrence with the same old occurrenceID (we don't want to duplicate occurrence).
				// This can happen if an occurrence was updated both singularly and with all the following with a recurrence.
				if updatedOcc.OldOccurrenceID == previousOldOccurrenceID {
					continue
				}

				occurrenceObj := ZoomMeetingOccurrence{
					OccurrenceID: updatedOcc.NewOccurrenceID, // stored in unix time as a string
					Title:        updatedOcc.Title,
					Description:  updatedOcc.Description,
					StartTime:    time.Unix(unixStartTime, 0).In(location).UTC().Format(time.RFC3339), // stored time as a formatted string
					Duration:     updatedOcc.Duration,
					IsCancelled:  false,
				}
				if updateOccAllFollowing != (UpdatedOccurrence{}) {
					occurrenceObj.Recurrence = updateOccAllFollowing.Recurrence
				}
				if occurrenceObj.Title == "" {
					occurrenceObj.Title = meeting.Title
				}
				if occurrenceObj.Description == "" {
					occurrenceObj.Description = meeting.Description
				}

				// Cancelled occurrences should have a status of "cancel" instead of "available".
				// This logic is for updated occurrences, so we must also
				// check within the cancelled occurrences for the new occurrence ID.
				if slices.Contains(meeting.CancelledOccurrences, occurrenceID) || slices.Contains(meeting.CancelledOccurrences, updatedOcc.NewOccurrenceID) {
					if !includeCancelled {
						continue
					}
					occurrenceObj.IsCancelled = true
				}

				previousOccurrence = occurrenceObj // set new previous occurrence
				previousOldOccurrenceID = updatedOcc.OldOccurrenceID
				logger.With("meeting_id", meeting.UID, "occurrence", occurrenceObj).DebugContext(ctx, "adding updated occurrence")
				result = append(result, occurrenceObj)
				// Return list of occurrences once the specific number of occurrences to return has been reached
				if len(result) == numOccurrencesToReturn {
					logger.With("meeting_id", meeting.UID, "time_elapsed_microseconds", time.Since(timerNow).Microseconds(), "num_occurrences", len(result)).DebugContext(ctx, "calculated meeting occurrences list")
					return result, nil
				}
			}

			// If occurrence is an updated occurrence then it was already included
			if isUpdated {
				logger.With("meeting_id", meeting.UID, "occurrence_id", o.Unix(), "occurrence_start_time", o).DebugContext(ctx, "occurrence already added")
				continue
			}

			actualStartTime := o.UTC().Format(time.RFC3339)
			if allFollowing && !currentStartTime.IsZero() {
				actualStartTime = time.Date(o.Year(), o.Month(), o.Day(), currentStartTime.Hour(), currentStartTime.Minute(), currentStartTime.Second(), 0, location).UTC().Format(time.RFC3339)
			}
			logger.With("meeting_id", meeting.UID, "adjusted_start_time", actualStartTime, "orig_start_time", o.Format(time.RFC3339), "is_adjusted", allFollowing && !currentStartTime.IsZero()).DebugContext(ctx, "occurrence after adjusting start time")
			// Use meeting duration unless this occurrence is part of an updated occurrence recurrence with a set duration
			actualDuration := meeting.Duration
			if allFollowing && currentDuration != 0 {
				actualDuration = currentDuration
			}

			// Skip past occurrences if no past occurrences are expected
			if !pastOccurrences && isOccurrencePast(o, actualDuration) {
				logger.With("meeting_id", meeting.UID, "occurrence_id", o.Unix(), "occurrence_start_time", o).DebugContext(ctx, "skipping past occurrence")
				continue
			}

			// We need to check below fields again here to ensure they are not empty
			if currentTitle == "" {
				currentTitle = meeting.Title
			}
			if currentDescription == "" {
				currentDescription = meeting.Description
			}

			actualStartTimeObj, _ := time.Parse(time.RFC3339, actualStartTime)
			occurrenceObj := ZoomMeetingOccurrence{
				OccurrenceID: strconv.FormatInt(actualStartTimeObj.Unix(), 10),
				StartTime:    actualStartTime,
				Duration:     actualDuration,
				IsCancelled:  false,
				Title:        currentTitle,
				Description:  currentDescription,
			}
			// Cancelled occurrences should have a status of "cancel" instead of "available",
			if slices.Contains(meeting.CancelledOccurrences, occurrenceID) {
				if !includeCancelled {
					continue
				}
				occurrenceObj.IsCancelled = true
			}
			previousOccurrence = occurrenceObj // set new previous occurrence
			logger.With("meeting_id", meeting.UID, "occurrence", occurrenceObj).DebugContext(ctx, "adding occurrence to list of occurrences")
			result = append(result, occurrenceObj)
			// Return list of occurrences once the specific number of occurrences to return has been reached
			if len(result) == numOccurrencesToReturn {
				logger.With("meeting_id", meeting.UID, "elapsed_time", time.Since(timerNow).String(), "num_occurrences", len(result)).DebugContext(ctx, "calculated meeting occurrences list")
				return result, nil
			}
		}
	}
	logger.With("meeting_id", meeting.UID, "elapsed_time", time.Since(timerNow).String(), "num_occurrences", len(result)).DebugContext(ctx, "calculated meeting occurrences list")

	return result, nil
}

func isOccurrencePast(startTime time.Time, duration int) bool {
	return startTime.Add(time.Duration(duration) * time.Minute).Add(meetingEndBuffer).Before(time.Now())
}

// timeInLocation returns error if name is invalid or empty.
// Otherwise, it returns the time for the given location. Example:
// if name == "Asia/Shanghai", returned time is in "Asia/Shanghai".
func timeInLocation(t time.Time, name string) (time.Time, error) {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.Time{}, err
	}

	return t.In(loc), err
}

// getRRuleOccurrences given a start time, optional timezone, and recurrence pattern, calculates and returns
// the list of occurrence times
func getRRuleOccurrences(startTime time.Time, timezone string, recurrence *ZoomMeetingRecurrence, endTime *time.Time) ([]time.Time, error) {
	rruleString, err := getRRule(*recurrence, endTime)
	if err != nil {
		return nil, err
	}

	if timezone != "" {
		startTime, err = timeInLocation(startTime, timezone)
		if err != nil {
			return nil, err
		}
	}

	set := rrule.Set{}
	r, err := rrule.StrToRRule(rruleString)
	if err != nil {
		return nil, err
	}
	r.DTStart(startTime)
	set.RRule(r)

	return set.All(), nil
}

// getRRule returns the recurrence rule for a meeting recurrence as a string
func getRRule(reccurrence ZoomMeetingRecurrence, endTime *time.Time) (string, error) {
	var rrule strings.Builder

	if reccurrence.Type < 1 || reccurrence.Type > 3 {
		return "", fmt.Errorf("invalid recurrence type: %d", reccurrence.Type)
	}

	rrule.WriteString(fmt.Sprintf("FREQ=%s;", strings.ToUpper(typeName[reccurrence.Type-1])))
	rrule.WriteString("WKST=SU;")

	if reccurrence.RepeatInterval != 0 {
		rrule.WriteString(fmt.Sprintf("INTERVAL=%d;", reccurrence.RepeatInterval))
	}

	if reccurrence.WeeklyDays != "" {
		s, err := parseByDay(reccurrence.WeeklyDays)
		if err != nil {
			return "", err
		}
		rrule.WriteString(fmt.Sprintf("BYDAY=%s;", s))
	} else if reccurrence.MonthlyWeek != 0 && reccurrence.MonthlyWeekDay != 0 {
		rrule.WriteString(fmt.Sprintf("BYDAY=%d%s;", reccurrence.MonthlyWeek, weekdaysABBRV[reccurrence.MonthlyWeekDay-1]))
	}

	if reccurrence.MonthlyDay != 0 {
		switch reccurrence.MonthlyDay {
		case 29:
			rrule.WriteString("BYMONTHDAY=28,29;BYSETPOS=-1;") // fall back to the 28th on months with 28 days if recurrence set to every 29th
		case 30:
			rrule.WriteString("BYMONTHDAY=28,29,30;BYSETPOS=-1;")
		case 31:
			rrule.WriteString("BYMONTHDAY=28,29,30,31;BYSETPOS=-1;")
		default:
			rrule.WriteString(fmt.Sprintf("BYMONTHDAY=%d;", reccurrence.MonthlyDay))
		}
	}

	if endTime != nil {
		rrule.WriteString(fmt.Sprintf("UNTIL=%s;", endTime.Format("20060102T150405Z")))
	} else {
		if reccurrence.EndDateTime != "" {
			reccurrence.EndTimes = 0
			t, err := time.Parse(time.RFC3339, reccurrence.EndDateTime)
			if err != nil {
				logger.With(errKey, err, "recurrence", reccurrence).Error("error parsing recurrence end_date_time")
				return "", err
			}
			rrule.WriteString(fmt.Sprintf("UNTIL=%s;", t.Format("20060102T150405Z")))
		}

		if reccurrence.EndTimes != 0 {
			rrule.WriteString(fmt.Sprintf("COUNT=%d;", reccurrence.EndTimes))
		}
	}

	return strings.TrimSuffix(rrule.String(), ";"), nil
}

// parseByDay takes a list of weekdays as a string and returns the list of
// abbreviations as a string where 1 is Sunday and 7 is Saturday
// (e.g. "2,3,6" -> "MO,TU,FR")
func parseByDay(days string) (string, error) {
	stringSlice := strings.Split(days, ",")
	var weekdays strings.Builder
	for i, item := range stringSlice {
		weekdayNum, err := strconv.Atoi(item)
		if err != nil {
			return "", err
		}
		// A weekday can only be 1-7. Skip numbers that are not in this range.
		if weekdayNum < 1 || weekdayNum > 7 {
			continue
		}
		// Except for the first weekday, there should be a comma before each subsequent weekday
		if i > 0 {
			weekdays.WriteString(",")
		}
		weekdays.WriteString(weekdaysABBRV[weekdayNum-1])
	}
	return weekdays.String(), nil
}
