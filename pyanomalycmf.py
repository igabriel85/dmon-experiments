from cm_api.api_client import ApiResource


cm_host = "85.120.206.95"
api = ApiResource(cm_host, username="admin", password="rexmundi220")

# Get a list of all clusters and check version
cdh4 = None
for c in api.get_all_clusters():
  print c.name
  print c.version
  if c.version == "CDH5":
    cdh5 = c

for s in cdh5.get_all_services():
    print s
    print s.type
    if s.type == 'HDFS':
        hdfs = s

print hdfs.name
print hdfs.serviceState
print hdfs.healthSummary
#print hdfs.update_config()