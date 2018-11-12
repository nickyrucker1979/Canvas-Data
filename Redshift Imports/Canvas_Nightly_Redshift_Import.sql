-- "C:\Program Files (x86)\Exasol\EXASolution-6.0\EXAplus\exaplus"
-- -c 10.132.13.17..18,10.132.13.20..21:8563 -u sys -p cuExa!sys1 -f "C:\Users\ruckern\CU_Online_Marketing_Scripts\CanvasData_Production\Canvas_Nightly_Redshift_Import.sql"

-- truncate tables
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ACCOUNT_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_GROUP_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_GROUP_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.CONVERSATION_MESSAGE_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.CONVERSATION_MESSAGE_PARTICIPANT_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_SCORE_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_SECTION_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_ENTRY_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_ENTRY_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_TOPIC_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_TOPIC_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_TERM_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.PSEUDONYM_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.USER_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_MEMBERSHIP_FACT;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_MEMBERSHIP_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_COMMENT_DIM;
truncate table CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_COMMENT_FACT;

-- Load tables
import into CANVAS_DATA_NIGHTLY_IMPORTS.ACCOUNT_DIM
(
  ID  ,
	CANVAS_ID  ,
	NAME  ,
	DEPTH  ,
	WORKFLOW_STATE  ,
	PARENT_ACCOUNT  ,
	PARENT_ACCOUNT_ID  ,
	GRANDPARENT_ACCOUNT  ,
	GRANDPARENT_ACCOUNT_ID  ,
	ROOT_ACCOUNT  ,
	ROOT_ACCOUNT_ID  ,
	SUBACCOUNT1  ,
	SUBACCOUNT1_ID  ,
	SUBACCOUNT2  ,
	SUBACCOUNT2_ID  ,
	SUBACCOUNT3  ,
	SUBACCOUNT3_ID  ,
	SUBACCOUNT4  ,
	SUBACCOUNT4_ID  ,
	SUBACCOUNT5  ,
	SUBACCOUNT5_ID  ,
	SUBACCOUNT6  ,
	SUBACCOUNT6_ID  ,
	SUBACCOUNT7  ,
	SUBACCOUNT7_ID  ,
	SUBACCOUNT8  ,
	SUBACCOUNT8_ID  ,
	SUBACCOUNT9  ,
	SUBACCOUNT9_ID  ,
	SUBACCOUNT10  ,
	SUBACCOUNT10_ID  ,
	SUBACCOUNT11  ,
	SUBACCOUNT11_ID  ,
	SUBACCOUNT12  ,
	SUBACCOUNT12_ID  ,
	SUBACCOUNT13  ,
	SUBACCOUNT13_ID  ,
	SUBACCOUNT14  ,
	SUBACCOUNT14_ID  ,
	SUBACCOUNT15  ,
	SUBACCOUNT15_ID  ,
	SIS_SOURCE_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        NAME  ,
        DEPTH  ,
        WORKFLOW_STATE  ,
        PARENT_ACCOUNT  ,
        PARENT_ACCOUNT_ID  ,
        GRANDPARENT_ACCOUNT  ,
        GRANDPARENT_ACCOUNT_ID  ,
        ROOT_ACCOUNT  ,
        ROOT_ACCOUNT_ID  ,
        SUBACCOUNT1  ,
        SUBACCOUNT1_ID  ,
        SUBACCOUNT2  ,
        SUBACCOUNT2_ID  ,
        SUBACCOUNT3  ,
        SUBACCOUNT3_ID  ,
        SUBACCOUNT4  ,
        SUBACCOUNT4_ID  ,
        SUBACCOUNT5  ,
        SUBACCOUNT5_ID  ,
        SUBACCOUNT6  ,
        SUBACCOUNT6_ID  ,
        SUBACCOUNT7  ,
        SUBACCOUNT7_ID  ,
        SUBACCOUNT8  ,
        SUBACCOUNT8_ID  ,
        SUBACCOUNT9  ,
        SUBACCOUNT9_ID  ,
        SUBACCOUNT10  ,
        SUBACCOUNT10_ID  ,
        SUBACCOUNT11  ,
        SUBACCOUNT11_ID  ,
        SUBACCOUNT12  ,
        SUBACCOUNT12_ID  ,
        SUBACCOUNT13  ,
        SUBACCOUNT13_ID  ,
        SUBACCOUNT14  ,
        SUBACCOUNT14_ID  ,
        SUBACCOUNT15  ,
        SUBACCOUNT15_ID  ,
        SIS_SOURCE_ID
      from account_dim'  -- redshift query to execute

ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - Account Dim - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;

import into CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_DIM
(
  ID  ,
  CANVAS_ID  ,
  COURSE_ID  ,
  TITLE  ,
  DESCRIPTION  ,
  DUE_AT  ,
  UNLOCK_AT  ,
  LOCK_AT  ,
  POINTS_POSSIBLE ,
  GRADING_TYPE ,
  SUBMISSION_TYPES  ,
  WORKFLOW_STATE  ,
  CREATED_AT  ,
  UPDATED_AT  ,
  PEER_REVIEW_COUNT  ,
  PEER_REVIEWS_DUE_AT  ,
  PEER_REVIEWS_ASSIGNED  ,
  PEER_REVIEWS  ,
  AUTOMATIC_PEER_REVIEWS  ,
  ALL_DAY  ,
  ALL_DAY_DATE ,
  COULD_BE_LOCKED  ,
  GRADE_GROUP_STUDENTS_INDIVIDUALLY  ,
  ANONYMOUS_PEER_REVIEWS  ,
  MUTED  ,
  ASSIGNMENT_GROUP_ID  ,
  FIELD_POSITION  ,
  VISIBILITY  ,
  EXTERNAL_TOOL_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        COURSE_ID  ,
        TITLE  ,
        DESCRIPTION  ,
        DUE_AT  ,
        UNLOCK_AT  ,
        LOCK_AT  ,
        cast(POINTS_POSSIBLE as varchar(100)),
        GRADING_TYPE  ,
        SUBMISSION_TYPES  ,
        WORKFLOW_STATE  ,
        CREATED_AT  ,
        UPDATED_AT  ,
        PEER_REVIEW_COUNT  ,
        PEER_REVIEWS_DUE_AT  ,
        PEER_REVIEWS_ASSIGNED  ,
        PEER_REVIEWS  ,
        AUTOMATIC_PEER_REVIEWS  ,
        ALL_DAY  ,
        ALL_DAY_DATE ,
        COULD_BE_LOCKED  ,
        GRADE_GROUP_STUDENTS_INDIVIDUALLY  ,
        ANONYMOUS_PEER_REVIEWS  ,
        MUTED  ,
        ASSIGNMENT_GROUP_ID  ,
        POSITION  ,
        VISIBILITY  ,
        EXTERNAL_TOOL_ID
      from assignment_dim'  -- redshift query to execute

ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - Assignment Dim - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

-- load assignment group dim

import into CANVAS_DATA_NIGHTLY_IMPORTS.ASSIGNMENT_GROUP_DIM
(
  ID  ,
  CANVAS_ID  ,
  COURSE_ID  ,
  NAME  ,
  DEFAULT_ASSIGNMENT_NAME  ,
  WORKFLOW_STATE  ,
  FIELD_POSITION  ,
  CREATED_AT  ,
  UPDATED_AT
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        COURSE_ID  ,
        NAME  ,
        DEFAULT_ASSIGNMENT_NAME  ,
        WORKFLOW_STATE  ,
        POSITION  ,
        CREATED_AT  ,
        UPDATED_AT
      from ASSIGNMENT_GROUP_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - ASSIGNMENT_GROUP_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

-- load conversation message dim
import into CANVAS_DATA_NIGHTLY_IMPORTS.CONVERSATION_MESSAGE_DIM
(
  ID  ,
  CANVAS_ID  ,
  CONVERSATION_ID  ,
  AUTHOR_ID  ,
  CREATED_AT  ,
  FIELD_GENERATED  ,
  HAS_ATTACHMENTS  ,
  HAS_MEDIA_OBJECTS  ,
  BODY
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        CONVERSATION_ID  ,
        AUTHOR_ID  ,
        CREATED_AT  ,
        GENERATED  ,
        HAS_ATTACHMENTS  ,
        HAS_MEDIA_OBJECTS  ,
        BODY
      from CONVERSATION_MESSAGE_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - CONVERSATION_MESSAGE_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;

import into CANVAS_DATA_NIGHTLY_IMPORTS.CONVERSATION_MESSAGE_PARTICIPANT_FACT
(
  CONVERSATION_MESSAGE_ID  ,
  CONVERSATION_ID  ,
  USER_ID  ,
  COURSE_ID  ,
  ENROLLMENT_TERM_ID  ,
  COURSE_ACCOUNT_ID  ,
  GROUP_ID  ,
  ACCOUNT_ID  ,
  ENROLLMENT_ROLLUP_ID  ,
  MESSAGE_SIZE_BYTES  ,
  MESSAGE_CHARACTER_COUNT  ,
  MESSAGE_WORD_COUNT  ,
  MESSAGE_LINE_COUNT
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        CONVERSATION_MESSAGE_ID  ,
        CONVERSATION_ID  ,
        USER_ID  ,
        COURSE_ID  ,
        ENROLLMENT_TERM_ID  ,
        COURSE_ACCOUNT_ID  ,
        GROUP_ID  ,
        ACCOUNT_ID  ,
        ENROLLMENT_ROLLUP_ID  ,
        MESSAGE_SIZE_BYTES  ,
        MESSAGE_CHARACTER_COUNT  ,
        MESSAGE_WORD_COUNT  ,
        MESSAGE_LINE_COUNT
      from CONVERSATION_MESSAGE_PARTICIPANT_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - CONVERSATION_MESSAGE_PARTICIPANT_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;

import into CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_DIM
(
  ID  ,
  CANVAS_ID  ,
  ROOT_ACCOUNT_ID  ,
  ACCOUNT_ID  ,
  ENROLLMENT_TERM_ID  ,
  NAME  ,
  CODE  ,
  TYPE  ,
  CREATED_AT  ,
  START_AT  ,
  CONCLUDE_AT  ,
  PUBLICLY_VISIBLE  ,
  SIS_SOURCE_ID  ,
  WORKFLOW_STATE  ,
  WIKI_ID  ,
  SYLLABUS_BODY

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        ROOT_ACCOUNT_ID  ,
        ACCOUNT_ID  ,
        ENROLLMENT_TERM_ID  ,
        NAME  ,
        CODE  ,
        TYPE  ,
        CREATED_AT  ,
        START_AT  ,
        CONCLUDE_AT  ,
        PUBLICLY_VISIBLE  ,
        SIS_SOURCE_ID  ,
        WORKFLOW_STATE  ,
        WIKI_ID  ,
        SYLLABUS_BODY

      from COURSE_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - COURSE_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;

import into CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_SCORE_FACT
(
  SCORE_ID  ,
  CANVAS_ID  ,
  ACCOUNT_ID ,
  COURSE_ID ,
  ENROLLMENT_ID,
  CURRENT_SCORE ,
  FINAL_SCORE ,
  MUTED_CURRENT_SCORE ,
  MUTED_FINAL_SCORE

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        SCORE_ID  ,
        CANVAS_ID  ,
        ACCOUNT_ID ,
        COURSE_ID ,
        ENROLLMENT_ID,
        cast(CURRENT_SCORE as varchar(100)),
        cast(FINAL_SCORE  as varchar(100)),
        cast(MUTED_CURRENT_SCORE  as varchar(100)),
        cast(MUTED_FINAL_SCORE as varchar(100))


      from COURSE_SCORE_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - COURSE_SCORE_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;


import into CANVAS_DATA_NIGHTLY_IMPORTS.COURSE_SECTION_DIM
(
  ID  ,
  CANVAS_ID  ,
  NAME  ,
  COURSE_ID  ,
  ENROLLMENT_TERM_ID  ,
  DEFAULT_SECTION  ,
  ACCEPTING_ENROLLMENTS  ,
  CAN_MANUALLY_ENROLL  ,
  START_AT  ,
  END_AT  ,
  CREATED_AT  ,
  UPDATED_AT  ,
  WORKFLOW_STATE  ,
  RESTRICT_ENROLLMENTS_TO_SECTION_DATES  ,
  NONXLIST_COURSE_ID  ,
  SIS_SOURCE_ID

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
          ID  ,
          CANVAS_ID  ,
          NAME  ,
          COURSE_ID  ,
          ENROLLMENT_TERM_ID  ,
          DEFAULT_SECTION  ,
          ACCEPTING_ENROLLMENTS  ,
          CAN_MANUALLY_ENROLL  ,
          START_AT  ,
          END_AT  ,
          CREATED_AT  ,
          UPDATED_AT  ,
          WORKFLOW_STATE  ,
          RESTRICT_ENROLLMENTS_TO_SECTION_DATES  ,
          NONXLIST_COURSE_ID  ,
          SIS_SOURCE_ID


      from COURSE_SECTION_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - COURSE_SECTION_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED

;

import into CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_ENTRY_DIM
(
  ID  ,
  CANVAS_ID  ,
  MESSAGE  ,
  WORKFLOW_STATE  ,
  CREATED_AT  ,
  UPDATED_AT  ,
  DELETED_AT  ,
  DEPTH

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        MESSAGE  ,
        WORKFLOW_STATE  ,
        CREATED_AT  ,
        UPDATED_AT  ,
        DELETED_AT  ,
        DEPTH
      from DISCUSSION_ENTRY_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - DISCUSSION_ENTRY_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_ENTRY_FACT
(
  DISCUSSION_ENTRY_ID  ,
  PARENT_DISCUSSION_ENTRY_ID  ,
  USER_ID  ,
  TOPIC_ID  ,
  COURSE_ID  ,
  ENROLLMENT_TERM_ID  ,
  COURSE_ACCOUNT_ID  ,
  TOPIC_USER_ID  ,
  TOPIC_ASSIGNMENT_ID  ,
  TOPIC_EDITOR_ID  ,
  ENROLLMENT_ROLLUP_ID  ,
  MESSAGE_LENGTH

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        DISCUSSION_ENTRY_ID  ,
        PARENT_DISCUSSION_ENTRY_ID  ,
        USER_ID  ,
        TOPIC_ID  ,
        COURSE_ID  ,
        ENROLLMENT_TERM_ID  ,
        COURSE_ACCOUNT_ID  ,
        TOPIC_USER_ID  ,
        TOPIC_ASSIGNMENT_ID  ,
        TOPIC_EDITOR_ID  ,
        ENROLLMENT_ROLLUP_ID  ,
        MESSAGE_LENGTH
      from DISCUSSION_ENTRY_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - DISCUSSION_ENTRY_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_TOPIC_DIM
(
	ID ,
	CANVAS_ID,
	TITLE,
	MESSAGE ,
	TYPE ,
	WORKFLOW_STATE ,
	LAST_REPLY_AT ,
	CREATED_AT,
	UPDATED_AT ,
	DELAYED_POST_AT ,
	POSTED_AT ,
	DELETED_AT ,
	DISCUSSION_TYPE ,
	PINNED ,
	LOCKED,
	COURSE_ID ,
	GROUP_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID ,
        CANVAS_ID,
        TITLE,
        MESSAGE ,
        TYPE ,
        WORKFLOW_STATE ,
        LAST_REPLY_AT ,
        CREATED_AT,
        UPDATED_AT ,
        DELAYED_POST_AT ,
        POSTED_AT ,
        DELETED_AT ,
        DISCUSSION_TYPE ,
        PINNED ,
        LOCKED,
        COURSE_ID ,
        GROUP_ID
      from discussion_topic_dim'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Manual - Discussion Topic Dim - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.DISCUSSION_TOPIC_FACT
(
  DISCUSSION_TOPIC_ID  ,
  COURSE_ID  ,
  ENROLLMENT_TERM_ID  ,
  COURSE_ACCOUNT_ID  ,
  USER_ID  ,
  ASSIGNMENT_ID  ,
  EDITOR_ID  ,
  ENROLLMENT_ROLLUP_ID  ,
  MESSAGE_LENGTH  ,
  GROUP_ID  ,
  GROUP_PARENT_COURSE_ID  ,
  GROUP_PARENT_ACCOUNT_ID  ,
  GROUP_PARENT_COURSE_ACCOUNT_ID

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        DISCUSSION_TOPIC_ID  ,
        COURSE_ID  ,
        ENROLLMENT_TERM_ID  ,
        COURSE_ACCOUNT_ID  ,
        USER_ID  ,
        ASSIGNMENT_ID  ,
        EDITOR_ID  ,
        ENROLLMENT_ROLLUP_ID  ,
        MESSAGE_LENGTH  ,
        GROUP_ID  ,
        GROUP_PARENT_COURSE_ID  ,
        GROUP_PARENT_ACCOUNT_ID  ,
        GROUP_PARENT_COURSE_ACCOUNT_ID
      from DISCUSSION_TOPIC_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - DISCUSSION_TOPIC_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_DIM
(
  ID  ,
  CANVAS_ID  ,
  ROOT_ACCOUNT_ID  ,
  COURSE_SECTION_ID  ,
  ROLE_ID  ,
  TYPE  ,
  WORKFLOW_STATE  ,
  CREATED_AT  ,
  UPDATED_AT  ,
  START_AT  ,
  END_AT  ,
  COMPLETED_AT  ,
  SELF_ENROLLED  ,
  SIS_SOURCE_ID  ,
  COURSE_ID  ,
  USER_ID  ,
  LAST_ACTIVITY_AT

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        ROOT_ACCOUNT_ID  ,
        COURSE_SECTION_ID  ,
        ROLE_ID  ,
        TYPE  ,
        WORKFLOW_STATE  ,
        CREATED_AT  ,
        UPDATED_AT  ,
        START_AT  ,
        END_AT  ,
        COMPLETED_AT  ,
        SELF_ENROLLED  ,
        SIS_SOURCE_ID  ,
        COURSE_ID  ,
        USER_ID  ,
        LAST_ACTIVITY_AT
      from ENROLLMENT_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - ENROLLMENT_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_FACT
(
  ENROLLMENT_ID  ,
  USER_ID  ,
  COURSE_ID  ,
  ENROLLMENT_TERM_ID  ,
  COURSE_ACCOUNT_ID  ,
  COURSE_SECTION_ID  ,
  COMPUTED_FINAL_SCORE  ,
  COMPUTED_CURRENT_SCORE

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ENROLLMENT_ID  ,
        USER_ID  ,
        COURSE_ID  ,
        ENROLLMENT_TERM_ID  ,
        COURSE_ACCOUNT_ID  ,
        COURSE_SECTION_ID  ,
        cast(COMPUTED_FINAL_SCORE as varchar(100)) ,
        cast(COMPUTED_CURRENT_SCORE as varchar(100))
      from ENROLLMENT_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - ENROLLMENT_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.ENROLLMENT_TERM_DIM
(
  ID  ,
  CANVAS_ID  ,
  ROOT_ACCOUNT_ID  ,
  NAME  ,
  DATE_START  ,
  DATE_END  ,
  SIS_SOURCE_ID

)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        ROOT_ACCOUNT_ID  ,
        NAME  ,
        DATE_START  ,
        DATE_END  ,
        SIS_SOURCE_ID
      from ENROLLMENT_TERM_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - ENROLLMENT_TERM_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.PSEUDONYM_DIM
(
  ID  ,
	CANVAS_ID  ,
	USER_ID  ,
	ACCOUNT_ID  ,
	WORKFLOW_STATE  ,
	LAST_REQUEST_AT  ,
	LAST_LOGIN_AT  ,
	CURRENT_LOGIN_AT  ,
	LAST_LOGIN_IP  ,
	CURRENT_LOGIN_IP  ,
	FIELD_POSITION  ,
	CREATED_AT  ,
	UPDATED_AT  ,
	PASSWORD_AUTO_FIELD_GENERATED  ,
	DELETED_AT  ,
	SIS_USER_ID  ,
	UNIQUE_NAME  ,
	INTEGRATION_ID  ,
	AUTHENTICATION_PROVIDER_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID  ,
        CANVAS_ID  ,
        USER_ID  ,
        ACCOUNT_ID  ,
        WORKFLOW_STATE  ,
        LAST_REQUEST_AT  ,
        LAST_LOGIN_AT  ,
        CURRENT_LOGIN_AT  ,
        LAST_LOGIN_IP  ,
        CURRENT_LOGIN_IP  ,
        POSITION  ,
        CREATED_AT  ,
        UPDATED_AT  ,
        PASSWORD_AUTO_GENERATED  ,
        DELETED_AT  ,
        SIS_USER_ID  ,
        UNIQUE_NAME  ,
        INTEGRATION_ID  ,
        AUTHENTICATION_PROVIDER_ID
      from PSEUDONYM_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - PSEUDONYM_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_DIM
(
	ID  ,
	CANVAS_ID  ,
	BODY ,
	URL ,
	GRADE ,
	SUBMITTED_AT  ,
	SUBMISSION_TYPE ,
	WORKFLOW_STATE ,
	CREATED_AT  ,
	UPDATED_AT  ,
	PROCESSED  ,
	PROCESS_ATTEMPTS  ,
	GRADE_MATCHES_CURRENT_SUBMISSION  ,
	PUBLISHED_GRADE ,
	GRADED_AT  ,
	HAS_RUBRIC_ASSESSMENT  ,
	ATTEMPT  ,
	HAS_ADMIN_COMMENT  ,
	ASSIGNMENT_ID  ,
	EXCUSED ,
	GRADED_ANONYMOUSLY ,
	GRADER_ID  ,
	GROUP_ID  ,
	QUIZ_SUBMISSION_ID  ,
	USER_ID  ,
	GRADE_STATE
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
				ID  ,
				CANVAS_ID  ,
				BODY ,
				URL ,
				GRADE ,
				SUBMITTED_AT  ,
				SUBMISSION_TYPE ,
				WORKFLOW_STATE ,
				CREATED_AT  ,
				UPDATED_AT  ,
				PROCESSED  ,
				PROCESS_ATTEMPTS  ,
				GRADE_MATCHES_CURRENT_SUBMISSION  ,
				PUBLISHED_GRADE ,
				GRADED_AT  ,
				HAS_RUBRIC_ASSESSMENT  ,
				ATTEMPT  ,
				HAS_ADMIN_COMMENT  ,
				ASSIGNMENT_ID  ,
				EXCUSED ,
				GRADED_ANONYMOUSLY ,
				GRADER_ID  ,
				GROUP_ID  ,
				QUIZ_SUBMISSION_ID  ,
				USER_ID  ,
				GRADE_STATE
      from SUBMISSION_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - SUBMISSION_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_FACT
(
	SUBMISSION_ID  ,
	ASSIGNMENT_ID  ,
	COURSE_ID  ,
	ENROLLMENT_TERM_ID  ,
	USER_ID  ,
	GRADER_ID  ,
	COURSE_ACCOUNT_ID  ,
	ENROLLMENT_ROLLUP_ID  ,
	SCORE  ,
	PUBLISHED_SCORE  ,
	WHAT_IF_SCORE  ,
	SUBMISSION_COMMENTS_COUNT  ,
	ACCOUNT_ID  ,
	ASSIGNMENT_GROUP_ID  ,
	GROUP_ID  ,
	QUIZ_ID  ,
	QUIZ_SUBMISSION_ID  ,
	WIKI_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
				SUBMISSION_ID  ,
				ASSIGNMENT_ID  ,
				COURSE_ID  ,
				ENROLLMENT_TERM_ID  ,
				USER_ID  ,
				GRADER_ID  ,
				COURSE_ACCOUNT_ID  ,
				ENROLLMENT_ROLLUP_ID  ,
				cast(SCORE as varchar(100)) ,
				cast(PUBLISHED_SCORE as varchar(100)) ,
				cast(WHAT_IF_SCORE as varchar(100)) ,
				SUBMISSION_COMMENTS_COUNT  ,
				ACCOUNT_ID  ,
				ASSIGNMENT_GROUP_ID  ,
				GROUP_ID  ,
				QUIZ_ID  ,
				QUIZ_SUBMISSION_ID  ,
				WIKI_ID
      from SUBMISSION_FACT'  -- redshift query to execute

ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - SUBMISSION_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

import into CANVAS_DATA_NIGHTLY_IMPORTS.USER_DIM
(
	ID  ,
	CANVAS_ID  ,
	ROOT_ACCOUNT_ID  ,
	NAME  ,
	"TIME_ZONE"  ,
	CREATED_AT  ,
	VISIBILITY  ,
	SCHOOL_NAME  ,
	SCHOOL_FIELD_POSITION  ,
	GENDER  ,
	LOCALE  ,
	PUBLIC  ,
	BIRTHFIELD_DATE  ,
	COUNTRY_CODE  ,
	WORKFLOW_STATE  ,
	SORTABLE_NAME  ,
	GLOBAL_CANVAS_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
				ID  ,
				CANVAS_ID  ,
				ROOT_ACCOUNT_ID  ,
				NAME  ,
				TIME_ZONE  ,
				CREATED_AT  ,
				VISIBILITY  ,
				SCHOOL_NAME  ,
				SCHOOL_POSITION  ,
				GENDER  ,
				LOCALE  ,
				PUBLIC  ,
				BIRTHDATE  ,
				COUNTRY_CODE  ,
				WORKFLOW_STATE  ,
				SORTABLE_NAME  ,
				GLOBAL_CANVAS_ID
      from USER_DIM'  -- redshift query to execute

ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - USER_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;


-- load group dim

import into CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_DIM
(
	ID,
	CANVAS_ID ,
	NAME ,
	DESCRIPTION ,
	CREATED_AT ,
	UPDATED_AT ,
	DELETED_AT ,
	IS_PUBLIC ,
	WORKFLOW_STATE ,
	CONTEXT_TYPE ,
	CATEGORY ,
	JOIN_LEVEL ,
	DEFAULT_VIEW ,
	SIS_SOURCE_ID ,
	GROUP_CATEGORY_ID ,
	ACCOUNT_ID ,
	WIKI_ID 
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID,
		CANVAS_ID ,
		NAME ,
		DESCRIPTION ,
		CREATED_AT ,
		UPDATED_AT ,
		DELETED_AT ,
		IS_PUBLIC ,
		WORKFLOW_STATE ,
		CONTEXT_TYPE ,
		CATEGORY ,
		JOIN_LEVEL ,
		DEFAULT_VIEW ,
		SIS_SOURCE_ID ,
		GROUP_CATEGORY_ID ,
		ACCOUNT_ID ,
		WIKI_ID 
      from GROUP_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - GROUP_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;


-- load group_membership_fact

import into CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_MEMBERSHIP_FACT
(
	GROUP_ID ,
	PARENT_COURSE_ID ,
	PARENT_ACCOUNT_ID ,
	PARENT_COURSE_ACCOUNT_ID ,
	ENROLLMENT_TERM_ID ,
	USER_ID ,
	GROUP_MEMBERSHIP_ID
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        GROUP_ID ,
        PARENT_COURSE_ID ,
        PARENT_ACCOUNT_ID ,
        PARENT_COURSE_ACCOUNT_ID ,
        ENROLLMENT_TERM_ID ,
        USER_ID ,
        GROUP_MEMBERSHIP_ID
      from GROUP_MEMBERSHIP_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - GROUP_MEMBERSHIP_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

-- load group_membership_dim

import into CANVAS_DATA_NIGHTLY_IMPORTS.GROUP_MEMBERSHIP_DIM
(
	ID ,
	CANVAS_ID ,
	GROUP_ID ,
	MODERATOR ,
	WORKFLOW_STATE ,
	CREATED_AT ,
	UPDATED_AT
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID ,
				CANVAS_ID ,
				GROUP_ID ,
				MODERATOR ,
				WORKFLOW_STATE ,
				CREATED_AT ,
				UPDATED_AT
      from GROUP_MEMBERSHIP_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - GROUP_MEMBERSHIP_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

-- load SUBMISSION_COMMENT_DIM

import into CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_COMMENT_DIM
(
	ID ,
	CANVAS_ID ,
	SUBMISSION_ID ,
	RECIPIENT_ID ,
	AUTHOR_ID ,
	ASSESSMENT_REQUEST_ID ,
	GROUP_COMMENT_ID ,
	COMMENT ,
	AUTHOR_NAME ,
	CREATED_AT ,
	UPDATED_AT ,
	ANONYMOUS ,
	TEACHER_ONLY_COMMENT ,
	HIDDEN
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        ID ,
        CANVAS_ID ,
        SUBMISSION_ID ,
        RECIPIENT_ID ,
        AUTHOR_ID ,
        ASSESSMENT_REQUEST_ID ,
        GROUP_COMMENT_ID ,
        COMMENT ,
        AUTHOR_NAME ,
        CREATED_AT ,
        UPDATED_AT ,
        ANONYMOUS ,
        TEACHER_ONLY_COMMENT ,
        HIDDEN
      from SUBMISSION_COMMENT_DIM'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - SUBMISSION_COMMENT_DIM - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;

-- load SUBMISSION_COMMENT_FACT

import into CANVAS_DATA_NIGHTLY_IMPORTS.SUBMISSION_COMMENT_FACT
(

	SUBMISSION_COMMENT_ID ,
	SUBMISSION_ID ,
	RECIPIENT_ID ,
	AUTHOR_ID ,
	ASSIGNMENT_ID ,
	COURSE_ID ,
	ENROLLMENT_TERM_ID ,
	COURSE_ACCOUNT_ID ,
	MESSAGE_SIZE_BYTES ,
	MESSAGE_CHARACTER_COUNT ,
	MESSAGE_WORD_COUNT ,
	MESSAGE_LINE_COUNT
)
from  jdbc at CANVAS_REDSHIFT -- virtual connection to Redshift (credentials stored in exasol)
      statement
      'select
        SUBMISSION_COMMENT_ID ,
        SUBMISSION_ID ,
        RECIPIENT_ID ,
        AUTHOR_ID ,
        ASSIGNMENT_ID ,
        COURSE_ID ,
        ENROLLMENT_TERM_ID ,
        COURSE_ACCOUNT_ID ,
        MESSAGE_SIZE_BYTES ,
        MESSAGE_CHARACTER_COUNT ,
        MESSAGE_WORD_COUNT ,
        MESSAGE_LINE_COUNT
      from SUBMISSION_COMMENT_FACT'  -- redshift query to execute


ERRORS INTO CANVAS_DATA_NIGHTLY_IMPORTS.ERROR_TABLE ('Redshift - SUBMISSION_COMMENT_FACT - '|| LEFT(CAST(current_timestamp as VARCHAR(100)),10)) REJECT LIMIT UNLIMITED
;