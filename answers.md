Why are the credentials stored in a Secret and not in the ConfigMap?
=> for them not to be leaked anywhere, a configmap is for non sensitive informations and is clear text that anyone that can access the configuration could read it , sensitive informations needs to be crypted 

The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?
=> If the job is executed again tomoroww it would fail. Once the secret has expired, the s3 backend rejects it.
A production platform would provide credentials to a job by rotating automaticaly the credentials and keeping the kubernetes secret updated

How would you turn this Job into a daily ingestion?
=> By scheduling a run everyday at the same time and being carefull to overlapping runs


The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?


The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?