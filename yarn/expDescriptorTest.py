test = {
	"exp1":
	[
		{
			"yarn": ["pi", "10", "100"],
			"cardinality": 1,
			"conf":{"hdfs": {"DATANODE": {"dfs_datanode_du_reserved": "8455053312"}},
					"yarn": {"NODEMANAGER": {"mapreduce_am_max-attempts": "2"}}}
		}
	]
}

for k, v in test.iteritems():
	#print k # experiment name
	#print v
	for r in v:
		#print r # experimental runs
		for ks, vs in r.iteritems():
			#print ks
			if ks == 'cardinality':
				print 'Cardinality is %s' %vs
			elif ks =='conf':
				#print 'Conf is %s' %vs
				for ck, cv in vs.iteritems():
					print ck
					print cv
			else:
				print 'job type is %s with parameters %s' %(ks, vs)
			#print vs 