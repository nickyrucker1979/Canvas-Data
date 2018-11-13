-- This query is used to unload data from our Redshift instance into our S3 bucket
UNLOAD ('select * from requests') --select from whatever table you are grabbing data from
to 's3://cuonline-canvas-data/requests'
access_key_id  'your_access_key_id' --can be generated in S3 portal
secret_access_key 'your_secret_access_key'
GZIP;
