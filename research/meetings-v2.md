# LFX v2 Meeting Service NATS KV Buckets Analysis

🤖 Generated with [GitHub Copilot](https://github.com/features/copilot) (via Zed)

## Overview

This document provides a comprehensive analysis of the NATS JetStream Key-Value (KV) store buckets used by the LFX v2 Meeting Service to represent meeting entities. The service uses 9 distinct NATS KV buckets to store different types of meeting-related data with a clean separation of concerns.

## NATS KV Bucket Architecture

### Bucket Summary

The LFX v2 Meeting Service uses the following NATS KV buckets:

| Bucket Name                 | Entity Type            | Description                                    |
|-----------------------------|------------------------|------------------------------------------------|
| `meetings`                  | MeetingBase            | Core meeting information and configuration     |
| `meeting-settings`          | MeetingSettings        | Meeting organizers and administrative settings |
| `meeting-registrants`       | Registrant             | Meeting registration records                   |
| `meeting-rsvps`             | RSVPResponse           | RSVP responses for meetings                    |
| `past-meetings`             | PastMeeting            | Historical meeting occurrence records          |
| `past-meeting-participants` | PastMeetingParticipant | Historical participant attendance data         |
| `past-meeting-recordings`   | PastMeetingRecording   | Meeting recording metadata and access URLs     |
| `past-meeting-transcripts`  | PastMeetingTranscript  | Meeting transcript content and metadata        |
| `past-meeting-summaries`    | PastMeetingSummary     | AI-generated meeting summaries                 |

## Detailed Bucket Analysis

### 1. `meetings` Bucket

**Entity Type:** `MeetingBase`
**Repository:** `NatsMeetingRepository`
**Key Pattern:** Direct meeting UID

**Purpose:** Stores core meeting information including scheduling, recurrence patterns, platform configuration, and basic metadata.

**Key Fields:**
- `uid`: Unique meeting identifier
- `project_uid`: Associated project
- `start_time`, `duration`, `timezone`: Scheduling information
- `recurrence`: Recurring meeting patterns
- `title`, `description`: Meeting content
- `committees`: Associated committees
- `platform`: Meeting platform (e.g., "Zoom")
- `zoom_config`: Platform-specific configuration
- `recording_enabled`, `transcript_enabled`: Feature flags
- `occurrences`: Generated meeting occurrences
- `registrant_count`: Current registration count

### 2. `meeting-settings` Bucket

**Entity Type:** `MeetingSettings`
**Repository:** `NatsMeetingRepository`
**Key Pattern:** Direct meeting UID

**Purpose:** Stores administrative settings and organizer information separately from core meeting data for better access control and modularity.

**Key Fields:**
- `uid`: Meeting identifier (matches MeetingBase)
- `organizers`: Array of organizer user IDs
- `created_at`, `updated_at`: Timestamps

### 3. `meeting-registrants` Bucket

**Entity Type:** `Registrant`
**Repository:** `NatsRegistrantRepository`
**Key Pattern:** Encoded keys using base64 encoding (e.g., `registrant/{uid}`)

**Purpose:** Stores meeting registration records with support for both direct registrations and committee-based registrations.

**Key Fields:**
- `uid`: Unique registrant identifier
- `meeting_uid`: Associated meeting
- `email`, `first_name`, `last_name`: Contact information
- `host`: Whether registrant is a meeting host
- `type`: Registration type ("direct" or "committee")
- `committee_uid`: Committee association (if applicable)
- `occurrence_id`: Specific occurrence registration
- `username`: Platform username

**Indexing Strategy:**
- Meeting index: `index/meeting/{meeting_uid}/{registrant_uid}`
- Email index: `index/email/{email}/{registrant_uid}`

### 4. `meeting-rsvps` Bucket

**Entity Type:** `RSVPResponse`
**Repository:** `NatsMeetingRSVPRepository`
**Key Pattern:** Direct RSVP ID

**Purpose:** Stores RSVP responses for meeting invitations.

**Key Fields:**
- `id`: Unique RSVP response identifier
- `meeting_uid`: Associated meeting
- Response details and timestamps

### 5. `past-meetings` Bucket

**Entity Type:** `PastMeeting`
**Repository:** `NatsPastMeetingRepository`
**Key Pattern:** Direct past meeting UID

**Purpose:** Stores historical records of completed meetings with session tracking and metadata preservation.

**Key Fields:**
- `uid`: Unique past meeting identifier
- `meeting_uid`: Original meeting reference
- `occurrence_id`: Specific occurrence identifier
- `project_uid`: Associated project
- `scheduled_start_time`, `scheduled_end_time`: Original schedule
- `sessions`: Array of actual start/end sessions
- `platform_meeting_id`: Platform-specific meeting ID
- `recording_uids`, `transcript_uids`, `summary_uids`: Related artifact references
- Complete preservation of original meeting configuration

### 6. `past-meeting-participants` Bucket

**Entity Type:** `PastMeetingParticipant`
**Repository:** `NatsPastMeetingParticipantRepository`
**Key Pattern:** Direct participant UID

**Purpose:** Records actual meeting attendance and participation data for historical analysis.

**Key Fields:**
- `uid`: Unique participant record identifier
- `past_meeting_uid`: Associated past meeting
- `email`: Participant email
- Attendance and participation metrics

### 7. `past-meeting-recordings` Bucket

**Entity Type:** `PastMeetingRecording`
**Repository:** `NatsPastMeetingRecordingRepository`
**Key Pattern:** Direct recording UID

**Purpose:** Stores meeting recording metadata, access URLs, and file information with session-based organization.

**Key Fields:**
- `uid`: Unique recording identifier
- `past_meeting_uid`: Associated past meeting
- `platform`: Recording platform
- `platform_meeting_instance_id`: Platform-specific instance ID
- Recording file metadata and access information

### 8. `past-meeting-transcripts` Bucket

**Entity Type:** `PastMeetingTranscript`
**Repository:** `NatsPastMeetingTranscriptRepository`
**Key Pattern:** Direct transcript UID

**Purpose:** Stores meeting transcript content and metadata.

**Key Fields:**
- `uid`: Unique transcript identifier
- `past_meeting_uid`: Associated past meeting
- `platform`: Source platform
- `platform_meeting_instance_id`: Platform-specific instance ID
- Transcript content and processing metadata

### 9. `past-meeting-summaries` Bucket

**Entity Type:** `PastMeetingSummary`
**Repository:** `NatsPastMeetingSummaryRepository`
**Key Pattern:** Direct summary UID

**Purpose:** Stores AI-generated meeting summaries and analysis.

**Key Fields:**
- `uid`: Unique summary identifier
- `past_meeting_uid`: Associated past meeting
- Summary content and generation metadata

## Key Management Strategy

### Encoding Strategy

The service uses two key encoding approaches:

1. **Direct Keys:** Simple entity UID as the key (used for most buckets)
2. **Encoded Keys:** Base64-encoded hierarchical keys (used for registrants with indexing)

### Key Builder Patterns

The `KeyBuilder` utility provides consistent key generation:

```text
// Entity keys
registrant/{uid}

// Index keys
index/{index_type}/{index_value}/{entity_uid}
```

### Indexing Strategy

**Registrant Indexing:**
- Meeting-based index for listing registrants by meeting
- Email-based index for duplicate detection and user lookup
- Base64 encoding handles special characters and NATS limitations

## Data Consistency and Relationships

### Meeting Lifecycle

1. **Meeting Creation:** Creates entries in both `meetings` and `meeting-settings` buckets
2. **Registration:** Creates `meeting-registrants` entries with appropriate indexing
3. **Meeting Occurrence:** Creates `past-meetings` entry when meeting starts
4. **Participation Tracking:** Creates `past-meeting-participants` entries during meeting
5. **Artifact Processing:** Creates recording, transcript, and summary entries post-meeting

### Cross-Bucket References

- `past-meetings` references original `meetings` via `meeting_uid`
- `past-meeting-participants` references `past-meetings` via `past_meeting_uid`
- `past-meeting-recordings`, `past-meeting-transcripts`, `past-meeting-summaries` all reference `past-meetings`
- `meeting-registrants` references `meetings` via `meeting_uid`

### Optimistic Concurrency Control

All repositories implement optimistic concurrency control using NATS KV revision numbers:
- `Get` operations return entity with revision
- `Update` and `Delete` operations require revision parameter
- Prevents concurrent modification conflicts

## Repository Architecture

### Base Repository Pattern

All repositories extend `NatsBaseRepository[T]` which provides:
- Generic CRUD operations
- Marshaling/unmarshaling
- Error handling with domain-specific errors
- Optimistic concurrency control
- Key listing and pattern matching

### Repository-Specific Features

**MeetingRepository:**
- Manages two buckets (`meetings` and `meeting-settings`)
- Coordinated create/update/delete operations
- Committee-based filtering

**RegistrantRepository:**
- Base64 key encoding
- Email uniqueness validation
- Meeting-based and email-based indexing
- Committee registration support

**Past Meeting Repositories:**
- Platform-specific lookups
- Session-based organization
- Cross-reference management

## Performance Considerations

### Scalability Features

- **Separate Buckets:** Logical separation improves query performance
- **Indexing Strategy:** Efficient lookups without full bucket scans
- **Encoded Keys:** Handle special characters and hierarchical organization
- **Base Repository Pattern:** Consistent error handling and marshaling

### Query Patterns

- **List Operations:** Full bucket scans with client-side filtering
- **Indexed Lookups:** Direct key access for registrant queries
- **Cross-References:** Multi-bucket queries for related data

## Error Handling

The service implements comprehensive error handling:

- `NotFoundError`: Entity not found in bucket
- `ConflictError`: Optimistic concurrency violations
- `ValidationError`: Invalid entity data
- `UnavailableError`: Service unavailability
- `InternalError`: Storage operation failures

## Security and Access Control

### Data Separation

- Meeting settings separated from base data for access control
- Historical data in separate buckets from active data
- Artifact metadata separated from content

### Key Encoding

- Base64 encoding prevents key injection attacks
- Consistent key patterns enable access pattern analysis

## Future Considerations

### Potential Optimizations

1. **Secondary Indexes:** Consider dedicated index buckets for complex queries
2. **Bucket Sharding:** Split large buckets by project or time-based criteria
3. **Caching Layer:** Add Redis caching for frequently accessed data
4. **Archive Strategy:** Move old past meeting data to separate archive buckets

### Migration Strategies

The current bucket design supports:
- Individual bucket migration without affecting others
- Gradual rollout of new features through bucket versioning
- Data consistency validation across related buckets

## Conclusion

The LFX v2 Meeting Service implements a well-architected NATS KV storage system with clear separation of concerns, consistent patterns, and robust error handling. The 9-bucket design provides excellent scalability while maintaining data consistency and supporting complex meeting lifecycle management.

The use of separate buckets for different entity types, combined with consistent repository patterns and optimistic concurrency control, creates a maintainable and performant storage layer suitable for enterprise-scale meeting management.
