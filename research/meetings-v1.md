# DynamoDB Tables Report for Zoom Service

🤖 Generated with [GitHub Copilot](https://github.com/features/copilot) (via Zed)

Based on analysis of the Zoom service code in `itx-service-zoom/zoom_meetings*.go` files and IAM permissions granted in serverless.yml files throughout the `itx-service-zoom/` directory, here's a comprehensive report on the DynamoDB tables actually used by the Zoom service (excluding `zoom_users` and `zoom_mock_users`):

## Tables with IAM Permissions (Actually Used)

The following tables have explicit IAM permissions granted in serverless.yml files across the service:

### Core Meeting Management

#### **itx-zoom-meetings-v2**
**Purpose:** Primary table for scheduled Zoom meetings  
**Used by:** Main service, event-handler, change-host-keys, terminate-meetings, invite-responses, webcal-feed-event, youtube-event-handler  
**Data Structure:** Based on the `zoomMeeting` struct, contains:
- Meeting metadata (topic, agenda, start time, timezone, duration)
- Meeting configuration (visibility, password, recording settings, AI features)
- Committee associations and filters
- Recurrence patterns and occurrences
- Registration and invite response tracking
- Audit information (created/modified timestamps and users)
- Job status tracking for bulk operations and mailing list syncing

#### **itx-zoom-meetings-registrants-v2**
**Purpose:** Meeting registrants and their registration details  
**Used by:** Main service, event-handler, invite-responses  
**Data Structure:** Based on the `ZoomMeetingRegistrant` struct, contains:
- Registrant identification (ID, user ID, email, username)
- Personal information (first name, last name, organization, job title)
- Registration metadata (type, committee association, host privileges)
- Email delivery tracking (delivery status, bounce information)
- Meeting occurrence associations
- Audit trail (created/modified timestamps and users)

#### **itx-zoom-meetings-mappings-v2**
**Purpose:** Meeting-to-project/committee mappings  
**Used by:** Main service, event-handler, webcal-feed-event

#### **itx-zoom-meetings-invite-responses-v2**
**Purpose:** Meeting invite responses (Yes/No/Maybe)  
**Used by:** Main service, invite-responses

### Past Meetings and Analytics

#### **itx-zoom-past-meetings**
**Purpose:** Completed meeting records with sessions and metadata  
**Used by:** Main service, event-handler, meetings-calculate-project-stats, youtube-event-handler  
**Data Structure:** Based on the `ZoomPastMeeting` struct, contains:
- Meeting metadata and scheduled times
- Actual session data (start/end times from Zoom)
- Recording and transcript settings
- AI summary configuration
- Meeting artifacts and YouTube links
- Audit trail

#### **itx-zoom-past-meetings-attendees**
**Purpose:** Who actually attended past meetings with session details  
**Used by:** Main service, meetings-calculate-project-stats

#### **itx-zoom-past-meetings-invitees**
**Purpose:** Who was invited to past meetings  
**Used by:** Main service, meetings-calculate-project-stats

#### **itx-zoom-past-meetings-recordings**
**Purpose:** Recording files and metadata for past meetings  
**Used by:** Main service, youtube-event-handler

#### **itx-zoom-past-meetings-summaries**
**Purpose:** AI-generated meeting summaries  
**Used by:** Main service, event-handler

#### **itx-zoom-past-meetings-mappings**
**Purpose:** Past meeting to project/committee mappings  
**Used by:** Main service

### Bulk Operations and Jobs

#### **itx-zoom-meetings-registrants-bulk-requests-v2**
**Purpose:** Bulk registrant import jobs  
**Used by:** Main service, event-handler

#### **itx-groupsio-v2-member-sync-jobs**
**Purpose:** Mailing list member synchronization jobs  
**Used by:** Main service, event-handler

### Statistics and Analytics

#### **itx-zoom-meetings-project-stats**
**Purpose:** Meeting statistics by project  
**Used by:** Main service, meetings-calculate-project-stats

#### **itx-zoom-meetings-audit**
**Purpose:** Audit log for meeting changes  
**Used by:** Main service, audit-log

### YouTube Integration

#### **itx-youtube-projects**
**Purpose:** YouTube integration for project channels  
**Used by:** youtube-event-handler

#### **itx-youtube-uploads**
**Purpose:** YouTube upload tracking for meeting recordings  
**Used by:** youtube-event-handler

### Additional Tables (Found in event-handler only)

#### **itx-zoom-meeting-files**
**Purpose:** Meeting file attachments and artifacts  
**Used by:** event-handler

## Tables Defined in zoom.tf vs. Actually Used

**Important Note:** The tables defined in `zoom.tf` (`itx-zoom-meetings` and `itx-zoom-meetings-registrants`) appear to be **legacy tables** that are NOT granted IAM permissions in any serverless.yml file. The service actually uses the **v2 versions** of these tables:

- `itx-zoom-meetings` (in terraform) → `itx-zoom-meetings-v2` (actually used)
- `itx-zoom-meetings-registrants` (in terraform) → `itx-zoom-meetings-registrants-v2` (actually used)

## Service Architecture Overview

The Zoom service is composed of multiple Lambda functions, each with specific responsibilities:

1. **Main Service** (`serverless.yml`) - Primary API endpoints and core functionality
2. **Event Handler** (`cmd/event-handler/`) - Webhook processing and async event handling
3. **Audit Log** (`cmd/audit-log/`) - Audit trail management
4. **Invite Responses** (`cmd/invite-responses/`) - Meeting RSVP processing
5. **Project Stats Calculator** (`cmd/meetings-calculate-project-stats/`) - Analytics generation
6. **YouTube Event Handler** (`cmd/youtube-event-handler/`) - Recording upload automation
7. **Webcal Feed Event** (`cmd/webcal-feed-event/`) - Calendar feed generation
8. **Meeting Terminator** (`cmd/terminate-meetings/`) - Cleanup operations
9. **Host Key Changer** (`cmd/change-host-keys/`) - Security management

## Key Service Capabilities

Based on the table usage patterns and service architecture:

1. **Scheduled Meetings:** Full lifecycle management with committee associations, recurrence patterns, and access controls
2. **Registration Management:** Direct registrants, committee-based registration, and bulk import capabilities  
3. **Past Meeting Analytics:** Comprehensive tracking of attendees, sessions, and meeting artifacts
4. **Recording and AI Features:** Recording access management, transcripts, and AI-powered summaries
5. **Email Delivery Tracking:** SES integration for invitation delivery monitoring and bounce handling
6. **YouTube Integration:** Automated upload of recordings to project YouTube channels
7. **Mailing List Integration:** Synchronization with Groups.io mailing lists for automatic registration
8. **Audit and Statistics:** Comprehensive tracking and reporting capabilities

## Migration Considerations for LFX v2

When migrating this data to LFX v2:

1. **Table Discrepancy:** The terraform definitions don't match the actual tables used by the service
2. **Schema Evolution:** The v2 tables suggest ongoing schema improvements and should be the source of truth
3. **Data Relationships:** Complex many-to-many relationships between meetings, projects, committees, and users
4. **Historical Data:** Extensive past meeting data with rich analytics capabilities
5. **Integration Points:** External service dependencies (YouTube, Groups.io, SES)
6. **Microservice Architecture:** Multiple specialized Lambda functions requiring coordinated migration
7. **Real-time Features:** Webhook-driven updates and job processing workflows

The current architecture demonstrates a mature meeting management system with comprehensive audit trails, analytics, and integration capabilities that should be preserved in any migration effort.