-- =============================================================================
-- Justice Nexus Migration Harness — Azure SQL Staging Schema
-- =============================================================================
-- Purpose : Defines the staging tables used as the landing zone before
--           records are promoted to the Justice Nexus target schema.
-- Database: JusticeNexus_Staging (Azure SQL / SQL Server 2019+)
-- Schema  : [stg] — isolated from operational tables
-- Conventions
--   * Every table carries standard audit columns (see bottom of each block).
--   * SourceId     = natural key from the originating system.
--   * MappingVersion = semantic version of the mapping artifact used.
--   * GoldenId     = FK to the resolved master entity after de-dup.
-- =============================================================================

USE [JusticeNexus_Staging];
GO

-- Drop schema first to allow idempotent re-runs in CI pipelines.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg')
    EXEC('CREATE SCHEMA [stg]');
GO

-- =============================================================================
-- 1. Stg_Contact
--    Individuals: defendants, victims, witnesses, attorneys, officers.
--    Sensitive PII fields are stored as hashes or encrypted values.
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Contact]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Contact];
GO

CREATE TABLE [stg].[Stg_Contact] (
    -- Surrogate / technical key
    ContactStagingId    BIGINT          IDENTITY(1,1)   NOT NULL,

    -- Source system linkage
    SourceSystem        NVARCHAR(50)    NOT NULL,           -- LEGACY_CASE | OPEN_JUSTICE | AD_HOC_EXPORTS
    SourceId            NVARCHAR(100)   NOT NULL,           -- PK from source system
    MappingVersion      NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    -- Identity fields (PII-masked where required)
    FullName            NVARCHAR(255)   NULL,               -- "LAST, FIRST MIDDLE"
    FirstName           NVARCHAR(100)   NULL,
    LastName            NVARCHAR(100)   NULL,
    MiddleName          NVARCHAR(100)   NULL,
    DOB                 DATE            NULL,               -- Date of birth
    SSN_Hash            CHAR(64)        NULL,               -- SHA-256 of SSN (hex, uppercase)
    GenderCode          NVARCHAR(10)    NULL,               -- M | F | X | U (unknown)
    RaceCode            NVARCHAR(20)    NULL,               -- mapped to DOJ NIBRS code
    EthnicityCode       NVARCHAR(20)    NULL,

    -- Contact details
    AddressLine1        NVARCHAR(255)   NULL,
    AddressLine2        NVARCHAR(100)   NULL,
    City                NVARCHAR(100)   NULL,
    StateCode           CHAR(2)         NULL,
    ZipCode             NVARCHAR(10)    NULL,
    CountyFIPS          CHAR(5)         NULL,               -- 5-digit FIPS code
    PhoneNumber         NVARCHAR(20)    NULL,
    EmailAddress        NVARCHAR(255)   NULL,

    -- Classification
    ContactTypeCode     NVARCHAR(30)    NULL,               -- DEFENDANT | VICTIM | WITNESS | ATTORNEY | OFFICER

    -- Golden record linkage (populated after de-duplication)
    GoldenId            BIGINT          NULL,               -- FK → master Contact table
    DuplicateFlag       BIT             NOT NULL DEFAULT 0,
    DuplicateOfId       BIGINT          NULL,

    -- Data quality
    ValidationStatus    NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',  -- PENDING | PASSED | FAILED
    ValidationNotes     NVARCHAR(MAX)   NULL,

    -- Audit
    CreatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId         NVARCHAR(36)    NULL,               -- migration job_id (UUID)

    CONSTRAINT PK_Stg_Contact PRIMARY KEY CLUSTERED (ContactStagingId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Contact_Source
    ON [stg].[Stg_Contact] (SourceSystem, SourceId, MappingVersion);

CREATE INDEX IX_Stg_Contact_GoldenId
    ON [stg].[Stg_Contact] (GoldenId)
    WHERE GoldenId IS NOT NULL;

CREATE INDEX IX_Stg_Contact_SSN_Hash
    ON [stg].[Stg_Contact] (SSN_Hash)
    WHERE SSN_Hash IS NOT NULL;

CREATE INDEX IX_Stg_Contact_LoadBatch
    ON [stg].[Stg_Contact] (LoadBatchId);
GO

-- =============================================================================
-- 2. Stg_Case
--    Criminal and civil case records mapped to the Justice Nexus case entity.
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Case]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Case];
GO

CREATE TABLE [stg].[Stg_Case] (
    CaseStagingId       BIGINT          IDENTITY(1,1)   NOT NULL,

    SourceSystem        NVARCHAR(50)    NOT NULL,
    SourceId            NVARCHAR(100)   NOT NULL,
    MappingVersion      NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    -- Case identifiers
    CaseNumber          NVARCHAR(100)   NULL,               -- External docket / case number
    CourtCaseNumber     NVARCHAR(100)   NULL,
    AgencyCaseNumber    NVARCHAR(100)   NULL,

    -- Case classification
    CaseTypeCode        NVARCHAR(50)    NULL,               -- FK → Stg_Code_CaseType
    JurisdictionCode    NVARCHAR(50)    NULL,               -- FK → Stg_Code_Jurisdiction
    OffenseCode         NVARCHAR(50)    NULL,               -- NIBRS / UCR code
    Statute             NVARCHAR(100)   NULL,

    -- Dates
    FilingDate          DATE            NULL,
    IncidentDate        DATE            NULL,
    ArrestDate          DATE            NULL,
    DispositionDate     DATE            NULL,
    SentenceDate        DATE            NULL,

    -- Outcome
    DispositionCode     NVARCHAR(50)    NULL,
    SentenceDescription NVARCHAR(MAX)   NULL,
    ConvictionFlag      BIT             NULL,

    -- Geography
    CountyFIPS          CHAR(5)         NULL,
    CourtName           NVARCHAR(255)   NULL,
    JudgeName           NVARCHAR(255)   NULL,

    -- Golden record
    GoldenId            BIGINT          NULL,

    -- Data quality
    ValidationStatus    NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',
    ValidationNotes     NVARCHAR(MAX)   NULL,

    -- Audit
    CreatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId         NVARCHAR(36)    NULL,

    CONSTRAINT PK_Stg_Case PRIMARY KEY CLUSTERED (CaseStagingId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Case_Source
    ON [stg].[Stg_Case] (SourceSystem, SourceId, MappingVersion);

CREATE INDEX IX_Stg_Case_CaseNumber
    ON [stg].[Stg_Case] (CaseNumber)
    WHERE CaseNumber IS NOT NULL;

CREATE INDEX IX_Stg_Case_JurisdictionCode
    ON [stg].[Stg_Case] (JurisdictionCode);

CREATE INDEX IX_Stg_Case_LoadBatch
    ON [stg].[Stg_Case] (LoadBatchId);
GO

-- =============================================================================
-- 3. Stg_Participant
--    Junction between a Contact and a Case with a role (defendant, victim…).
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Participant]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Participant];
GO

CREATE TABLE [stg].[Stg_Participant] (
    ParticipantStagingId    BIGINT          IDENTITY(1,1)   NOT NULL,

    SourceSystem            NVARCHAR(50)    NOT NULL,
    SourceId                NVARCHAR(100)   NOT NULL,
    MappingVersion          NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    -- Resolved staging FKs (populated after Contact and Case have been staged)
    ContactStagingId        BIGINT          NULL,
    CaseStagingId           BIGINT          NULL,

    -- Source system raw keys (before resolution)
    SourceContactId         NVARCHAR(100)   NULL,
    SourceCaseId            NVARCHAR(100)   NULL,

    -- Role
    ParticipantRoleCode     NVARCHAR(50)    NULL,           -- DEFENDANT | VICTIM | CO-DEFENDANT | …
    ParticipantRoleDesc     NVARCHAR(255)   NULL,

    -- Case-specific attributes for this participant
    ChargeCode              NVARCHAR(50)    NULL,
    ChargeDescription       NVARCHAR(255)   NULL,
    SeverityCode            NVARCHAR(20)    NULL,           -- FELONY | MISDEMEANOR | INFRACTION
    CountNumber             SMALLINT        NULL,
    DispositionCode         NVARCHAR(50)    NULL,

    -- Golden record
    GoldenId                BIGINT          NULL,

    -- Audit
    CreatedAt               DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt               DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId             NVARCHAR(36)    NULL,

    CONSTRAINT PK_Stg_Participant PRIMARY KEY CLUSTERED (ParticipantStagingId),
    CONSTRAINT FK_Stg_Participant_Contact
        FOREIGN KEY (ContactStagingId)
        REFERENCES [stg].[Stg_Contact] (ContactStagingId),
    CONSTRAINT FK_Stg_Participant_Case
        FOREIGN KEY (CaseStagingId)
        REFERENCES [stg].[Stg_Case] (CaseStagingId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Participant_Source
    ON [stg].[Stg_Participant] (SourceSystem, SourceId, MappingVersion);

CREATE INDEX IX_Stg_Participant_Contact
    ON [stg].[Stg_Participant] (ContactStagingId);

CREATE INDEX IX_Stg_Participant_Case
    ON [stg].[Stg_Participant] (CaseStagingId);

CREATE INDEX IX_Stg_Participant_LoadBatch
    ON [stg].[Stg_Participant] (LoadBatchId);
GO

-- =============================================================================
-- 4. Stg_Code_Jurisdiction
--    Reference / lookup table mapping source jurisdiction codes to DOJ standard.
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Code_Jurisdiction]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Code_Jurisdiction];
GO

CREATE TABLE [stg].[Stg_Code_Jurisdiction] (
    JurisdictionCodeId  BIGINT          IDENTITY(1,1)   NOT NULL,

    SourceSystem        NVARCHAR(50)    NOT NULL,
    SourceCode          NVARCHAR(50)    NOT NULL,           -- Raw code from source
    MappingVersion      NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    -- Mapped target value
    TargetCode          NVARCHAR(50)    NULL,               -- DOJ standard jurisdiction code
    TargetDescription   NVARCHAR(255)   NULL,

    -- Mapping metadata
    ConfidenceScore     DECIMAL(5,4)    NULL,               -- 0.0000 – 1.0000
    MappingStatus       NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',   -- APPROVED | PENDING | REJECTED
    MappingNotes        NVARCHAR(MAX)   NULL,
    ReviewedBy          NVARCHAR(100)   NULL,
    ReviewedAt          DATETIME2(7)    NULL,

    -- Audit
    CreatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId         NVARCHAR(36)    NULL,

    CONSTRAINT PK_Stg_Code_Jurisdiction PRIMARY KEY CLUSTERED (JurisdictionCodeId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Code_Jurisdiction_Source
    ON [stg].[Stg_Code_Jurisdiction] (SourceSystem, SourceCode, MappingVersion);
GO

-- =============================================================================
-- 5. Stg_Code_CaseType
--    Maps source case-type codes to the Justice Nexus case-type taxonomy.
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Code_CaseType]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Code_CaseType];
GO

CREATE TABLE [stg].[Stg_Code_CaseType] (
    CaseTypeCodeId      BIGINT          IDENTITY(1,1)   NOT NULL,

    SourceSystem        NVARCHAR(50)    NOT NULL,
    SourceCode          NVARCHAR(50)    NOT NULL,
    MappingVersion      NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    TargetCode          NVARCHAR(50)    NULL,
    TargetDescription   NVARCHAR(255)   NULL,
    IsViolent           BIT             NULL,               -- DOJ violent-crime flag
    IsPropertyCrime     BIT             NULL,

    ConfidenceScore     DECIMAL(5,4)    NULL,
    MappingStatus       NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',
    MappingNotes        NVARCHAR(MAX)   NULL,
    ReviewedBy          NVARCHAR(100)   NULL,
    ReviewedAt          DATETIME2(7)    NULL,

    CreatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId         NVARCHAR(36)    NULL,

    CONSTRAINT PK_Stg_Code_CaseType PRIMARY KEY CLUSTERED (CaseTypeCodeId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Code_CaseType_Source
    ON [stg].[Stg_Code_CaseType] (SourceSystem, SourceCode, MappingVersion);
GO

-- =============================================================================
-- 6. Stg_Code_EventType
--    Maps source event/activity type codes (hearings, filings, dispositions).
-- =============================================================================
IF OBJECT_ID(N'[stg].[Stg_Code_EventType]', N'U') IS NOT NULL
    DROP TABLE [stg].[Stg_Code_EventType];
GO

CREATE TABLE [stg].[Stg_Code_EventType] (
    EventTypeCodeId     BIGINT          IDENTITY(1,1)   NOT NULL,

    SourceSystem        NVARCHAR(50)    NOT NULL,
    SourceCode          NVARCHAR(50)    NOT NULL,
    MappingVersion      NVARCHAR(20)    NOT NULL DEFAULT '1.0.0',

    TargetCode          NVARCHAR(50)    NULL,
    TargetDescription   NVARCHAR(255)   NULL,
    EventCategory       NVARCHAR(50)    NULL,               -- HEARING | FILING | DISPOSITION | SENTENCE

    ConfidenceScore     DECIMAL(5,4)    NULL,
    MappingStatus       NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',
    MappingNotes        NVARCHAR(MAX)   NULL,
    ReviewedBy          NVARCHAR(100)   NULL,
    ReviewedAt          DATETIME2(7)    NULL,

    CreatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    LoadBatchId         NVARCHAR(36)    NULL,

    CONSTRAINT PK_Stg_Code_EventType PRIMARY KEY CLUSTERED (EventTypeCodeId)
);
GO

CREATE UNIQUE INDEX UX_Stg_Code_EventType_Source
    ON [stg].[Stg_Code_EventType] (SourceSystem, SourceCode, MappingVersion);
GO

-- =============================================================================
-- Trigger: auto-update UpdatedAt on Stg_Contact
-- =============================================================================
CREATE OR ALTER TRIGGER [stg].[trg_Stg_Contact_UpdatedAt]
ON [stg].[Stg_Contact]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE c
    SET    c.UpdatedAt = SYSUTCDATETIME()
    FROM   [stg].[Stg_Contact] c
    INNER JOIN inserted i ON c.ContactStagingId = i.ContactStagingId;
END;
GO

-- =============================================================================
-- Trigger: auto-update UpdatedAt on Stg_Case
-- =============================================================================
CREATE OR ALTER TRIGGER [stg].[trg_Stg_Case_UpdatedAt]
ON [stg].[Stg_Case]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE c
    SET    c.UpdatedAt = SYSUTCDATETIME()
    FROM   [stg].[Stg_Case] c
    INNER JOIN inserted i ON c.CaseStagingId = i.CaseStagingId;
END;
GO
