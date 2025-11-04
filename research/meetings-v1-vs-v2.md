# LFX Meetings v1 vs v2 Schema Comparison and Migration Analysis

🤖 Generated with [GitHub Copilot](https://github.com/features/copilot) (via Zed)

## Executive Summary

This document analyzes the schema differences between LFX v1 (DynamoDB-based Zoom service) and LFX v2 (NATS KV-based Meeting service) to assist with data migration planning. The analysis focuses on shared features, schema transformations, and critical identity mapping requirements, particularly around the deprecated v1 platform IDs that must be resolved to LFID usernames or emails in v2.

## Storage Architecture Comparison

### LFX v1 Architecture
- **Storage:** DynamoDB tables
- **Tables:** 18 distinct tables with complex relationships
- **Architecture:** Microservice with multiple Lambda functions
- **Identity:** Platform IDs (contact IDs/SFIDs) from user-service

### LFX v2 Architecture
- **Storage:** NATS JetStream Key-Value buckets
- **Buckets:** 9 distinct buckets with clean separation
- **Architecture:** Single service with domain-driven design
- **Identity:** LFID usernames (registered users) and emails (unregistered users)

## Core Entity Mapping

### Meeting Entities

| Feature | v1 Table | v2 Bucket | Migration Complexity |
|---------|----------|-----------|---------------------|
| Core meetings | `itx-zoom-meetings-v2` | `meetings` | **High** - Schema transformation |
| Meeting settings | Embedded in core table | `meeting-settings` | **Medium** - Data extraction |
| Registrants | `itx-zoom-meetings-registrants-v2` | `meeting-registrants` | **High** - Identity mapping |
| Mappings | `itx-zoom-meetings-mappings-v2` | Embedded in `meetings` | **Medium** - Data restructuring |
| Past meetings | `itx-zoom-past-meetings` | `past-meetings` | **Medium** - Schema alignment |
| Attendees | `itx-zoom-past-meetings-attendees` | `past-meeting-participants` | **High** - Identity mapping |
| Recordings | `itx-zoom-past-meetings-recordings` | `past-meeting-recordings` | **Low** - Minor schema changes |

## Critical Identity Mapping Requirements

### Platform ID Dependencies

The following v1 entities contain platform IDs that require resolution to usernames/emails:

#### 1. Meeting Registrants (`itx-zoom-meetings-registrants-v2`)

**v1 Schema (platform ID fields):**
- `user_id` - Platform ID from user-service (deprecated)
- `created_by` - Platform ID of user who created registration
- `modified_by` - Platform ID of user who modified registration

**v2 Schema (identity fields):**
- `username` - LFID username for registered users
- `email` - Email address for unregistered users
- No audit fields store platform IDs

**Migration Requirements:**
- Map `user_id` → `username` or `email` via user-service lookup
- Map `created_by`/`modified_by` → audit system (outside migration scope)
- Validate email uniqueness per meeting in v2

#### 2. Past Meeting Attendees (`itx-zoom-past-meetings-attendees`)

**v1 Schema (platform ID fields):**
- Contains platform IDs for attendee identification
- May lack email addresses for some attendees

**v2 Schema (identity fields):**
- `email` - Primary identifier for participants
- No platform ID fields

**Migration Requirements:**
- Map all platform IDs → emails via user-service
- Handle cases where user-service lacks email for platform ID
- Consider data loss for unmappable platform IDs

#### 3. Meeting Organizers

**v1 Schema:**
- Organizers embedded in meeting record as platform IDs
- `created_by`/`modified_by` audit fields use platform IDs

**v2 Schema:**
- `meeting-settings.organizers` - Array of user IDs (usernames)
- Audit fields not migrated

**Migration Requirements:**
- Map organizer platform IDs → LFID usernames
- Default to email if username unavailable
- Handle orphaned meetings where organizers cannot be mapped

## Schema Transformation Details

### Core Meeting Schema

#### v1 `itx-zoom-meetings-v2` → v2 `meetings`

**Direct Mappings:**
```
v1 Field → v2 Field
topic → title
agenda → description
start_time → start_time
duration → duration
timezone → timezone
password → zoom_config.password
registration_enabled → (derived from registrant_count > 0)
recording_enabled → recording_enabled
```

**Complex Transformations:**
- `committee_filters` (v1 array) → `committees` (v2 array) - Structure change
- Recurrence pattern serialization differences
- Meeting visibility rules restructured
- Project associations moved from separate mapping table to embedded field

#### v1 `itx-zoom-meetings-registrants-v2` → v2 `meeting-registrants`

**Identity Resolution Required:**
```
v1.user_id (platform ID) → v2.username OR v2.email
```

**Field Mappings:**
```
v1 Field → v2 Field
id → uid
meeting_id → meeting_uid
email → email (if user_id unmappable)
first_name → first_name
last_name → last_name
organization → (not in v2 schema)
job_title → (not in v2 schema)
registrant_type → type
committee_id → committee_uid
host → host
occurrence_id → occurrence_id
```

**Data Loss:**
- `organization` and `job_title` not preserved in v2
- Email delivery tracking (`delivery_status`, `bounce_info`) not in v2
- Rich audit trail reduced to basic timestamps

### Past Meetings Schema

#### v1 `itx-zoom-past-meetings` → v2 `past-meetings`

**Session Structure Change:**
- v1: Single start/end time per meeting
- v2: Array of sessions with multiple start/end pairs
- Migration must wrap single session in array

**Field Mappings:**
```
v1 Field → v2 Field
id → uid
meeting_id → meeting_uid
occurrence_id → occurrence_id
scheduled_start_time → scheduled_start_time
scheduled_end_time → scheduled_end_time
actual_start_time → sessions[0].start_time
actual_end_time → sessions[0].end_time
zoom_meeting_id → platform_meeting_id
recording_enabled → (preserved from original meeting)
```

#### v1 `itx-zoom-past-meetings-attendees` → v2 `past-meeting-participants`

**Critical Identity Mapping:**
```
v1.attendee_platform_id → v2.email (via user-service lookup)
```

**Attendance Data:**
- v1: Rich attendance tracking with join/leave times
- v2: Simplified participation model
- Consider aggregating detailed attendance into summary metrics

## Data Migration Challenges

### High Priority Issues

#### 1. Platform ID Resolution
- **Problem:** v1 extensively uses platform IDs, v2 uses usernames/emails
- **Solution:** Comprehensive user-service API integration required
- **Risk:** Data loss for unmappable platform IDs
- **Mitigation:** Maintain platform ID mapping table for audit trail

#### 2. Schema Complexity Reduction
- **Problem:** v2 has simpler schemas, some v1 data won't transfer
- **Examples:** Organization/job title in registrants, detailed audit trails
- **Solution:** Document data loss and consider archival storage

#### 3. Storage Architecture Differences
- **Problem:** DynamoDB patterns don't map to NATS KV
- **Examples:** Complex indexes, batch operations, conditional updates
- **Solution:** Application-level logic in migration tooling

### Medium Priority Issues

#### 1. Recurrence Pattern Changes
- v1 and v2 have different recurrence serialization
- Requires pattern transformation logic

#### 2. Committee Association Changes
- v1: Separate mapping table
- v2: Embedded in meeting entity
- Requires data denormalization

#### 3. Meeting Settings Separation
- v1: All data in single table
- v2: Separate settings bucket
- Requires data splitting logic

## Required External Data Sources

### User Service Mappings
To resolve platform IDs, migration tooling must access:

1. **Platform ID → Username mapping**
   - For registered LFX users with LFID accounts
   - Primary identity in v2 system

2. **Platform ID → Email mapping**
   - For unregistered users or username resolution failures
   - Fallback identity in v2 system

3. **User Profile Data**
   - To "hydrate" profiles into per-record (decoupled) entries, if determined necessary

### Project/Committee Mappings
- Resolve committee IDs between v1 and v2 systems
- Validate project associations remain valid
- Handle organizational restructuring

## Data Quality Considerations

### Pre-Migration Validation
- Identify records with missing platform IDs
- Validate email format consistency
- Check for duplicate registrations per meeting
- Verify meeting occurrence consistency

### Post-Migration Validation
- Verify all platform IDs were resolved
- Check meeting registrant counts match
- Validate past meeting session data integrity
- Confirm project/committee associations

### Rollback Planning
- Maintain original v1 data during migration
- Create point-in-time snapshots before migration
- Document all transformation decisions
- Build rollback procedures for critical failures
