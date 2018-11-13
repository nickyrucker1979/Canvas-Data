select distinct e.type, ud.sortable_name as name, cc.address as email
from enrollment_dim e
left join enrollment_fact ef
  on e.id = ef.enrollment_id
  and (e.role_id = '10430000000000185' or e.role_id = '10430000000000184') --get TaEnrollment and TeacherEnrollment
left join communication_channel_dim cc
  on cc.user_id = e.user_id
join user_dim ud
  on cc.user_id = ud.id
join enrollment_term_dim term
  on ef.enrollment_term_id = term.id
where (ef.enrollment_term_id = '10430000000000085' ) --get Spring 2017 and Fall 2016
  and cc.position = '1'
  and cc.type = 'email'
  and term.name = 'Summer 2018'
;
