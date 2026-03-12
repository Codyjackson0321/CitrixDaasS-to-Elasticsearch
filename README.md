# CitrixDaasS-to-Elasticsearch

### Description
This super simple script is designed to take logs from Citrix DaaS and import it into Elasticsearch in a format that is close enough to ECS to be usable/functional.


### Limtiations
This isn't designed for large scale usage and logging of Citrix DaaS. This is basically a minimally viable option to get a small environment of data into Elastic. If you need it to scale let me know and I'll update it.



### Required Environment variables
```
ELASTIC_URL
ELASTIC_API_KEY
CITRIX_CLIENT_ID
CITRIX_CLIENT_SECRET
CUSTOMER_ID
SITE_ID
```

### Requirements
- Python 3.11
- requests
- elasticsearch
- dotenv


### Setup
To keep things simple, we just run this on a cron job.

```bash
chmod +x /path/to/CitrixDaaSHarvester.py
crontab -e
```

Input the following value:

```
*/10 * * * * /path/to/python /path/to/CitrixDaaSHarvester.py
```