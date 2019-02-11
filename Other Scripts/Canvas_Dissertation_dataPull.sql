
; with cte as
  (select
  CANVAS_USER_ID,
  CANVAS_USER_UI_ID,
  USER_NAME,
  ENROLLMENT_TYPE,
  SCHOOL_COLLEGE,
  CANVAS_COURSE_ID,
  CANVAS_COURSE_UI_ID,
  CANVAS_SECTION_ID,
  COURSE_NAME,
  COURSE_SECTION_NAME,
  INSTRUCTION_MODE
from
  CANVAS_ENROLLMENTS_VIEW

where
  SIS_TERM = '2171'
  and ENROLLMENT_STATE = 'active'

)
, teachers as
    (
    select
      c1.CANVAS_USER_ID,
      c1.CANVAS_USER_UI_ID,
      c1.USER_NAME,
      c1.ENROLLMENT_TYPE,
      c1.SCHOOL_COLLEGE,
      c1.CANVAS_COURSE_ID,
      c1.CANVAS_COURSE_UI_ID,
      c1.CANVAS_SECTION_ID,
      c1.COURSE_NAME,
      c1.COURSE_SECTION_NAME,
      c1.INSTRUCTION_MODE
    from
      cte c1
    where
      c1.ENROLLMENT_TYPE = 'Teacher'
  )
, enrollment_counts as
    (
      select
        c2.CANVAS_COURSE_ID,
        c2.CANVAS_SECTION_ID,
        sum(case when c2.ENROLLMENT_TYPE = 'Teacher'
          then 1
            else 0 end) as TEACHER_COUNT,
        sum(case when c2.ENROLLMENT_TYPE = 'Ta'
          then 1
            else 0 end) as TA_COUNT,
        sum(case when c2.ENROLLMENT_TYPE = 'Student'
          then 1
            else 0 end) as STUDENT_COUNT
      from
        cte c2
      group by
        c2.CANVAS_COURSE_ID,
        c2.CANVAS_SECTION_ID
    )

select
  t.*,
  e.TEACHER_COUNT,
  e.TA_COUNT,
  e.STUDENT_COUNT

from
  teachers t
   join enrollment_counts e
    on t.CANVAS_COURSE_ID = e.CANVAS_COURSE_ID and t.CANVAS_SECTION_ID = e.CANVAS_SECTION_ID

order by CANVAS_USER_UI_ID
;
