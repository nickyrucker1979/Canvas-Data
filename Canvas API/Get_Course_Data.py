import requests

domain = 'ucdenver.instructure.com'
token = ''
course_id = '' # course id

response = requests.get('https://'+domain+'/api/v1/courses/'+course_id+'/analytics/activity', headers={'Authorization': 'Bearer '+token})
print response.status_code
print response.json()
