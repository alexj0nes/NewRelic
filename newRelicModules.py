import pandas
from newrelic_api import Applications
import os
import datetime
from datetime import date, timedelta
import time
from functools import wraps

# Define New Relic API Key
NEW_RELIC_API_KEY = str(os.environ['NEW_RELIC_API_KEY'])
app = Applications(api_key=NEW_RELIC_API_KEY)

# Define LOCAL_PATH
LOCAL_PATH = 'T:\\Warehouse Data Files\\New Relic\\'

# Define appListFileName
appListFileName = LOCAL_PATH+'applicationList.csv'

# Define appMetricsDataFileName
appMetricsDataFileName = LOCAL_PATH+'appMetricsData.csv'

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

def getApplicationList():
	# Get list of application ids and names
	print 'Getting list of applications'

	appsColumnNames = ['id', 'name', 'language', 'health_status', 'reporting', 'last_reported_at', 'response_time',
					   'throughput', 'error_rate', 'apdex_target', 'host_count', 'concurrent_instance_count',
					   'app_apdex_threshold', 'end_user_apdex_threshold', 'enable_real_user_monitoring',
					   'use_server_side_config', 'application_instances', 'servers', 'application_hosts']

	# Create empty pandas dataFrame to hold application ids and names
	appsDF = pandas.DataFrame(columns=appsColumnNames)
	appsDF = appsDF.truncate()

	for j in range(1, 100):
		response = app.list(page=j)
		if len(response['applications']) == 0:
			break
		else:
			print 'Getting page ' + str(j)
			# Create empty pandas dataFrame to hold cursor
			appsDFTemp = pandas.DataFrame(index=range(len(response['applications'])), columns=appsColumnNames)
			for i in range(len(response['applications'])):
				appsDFTemp['id'][i] = response['applications'][i]['id']
				appsDFTemp['name'][i] = response['applications'][i]['name']
				appsDFTemp['language'][i] = response['applications'][i]['language']
				appsDFTemp['health_status'][i] = response['applications'][i]['health_status']
				appsDFTemp['reporting'][i] = response['applications'][i]['reporting']
				if response['applications'][i]['reporting']:
					appsDFTemp['last_reported_at'][i] = response['applications'][i]['last_reported_at']
					appsDFTemp['response_time'][i] = response['applications'][i]['application_summary']['response_time']
					appsDFTemp['throughput'][i] = response['applications'][i]['application_summary']['throughput']
					appsDFTemp['error_rate'][i] = response['applications'][i]['application_summary']['error_rate']
					appsDFTemp['apdex_target'][i] = response['applications'][i]['application_summary']['apdex_target']
					appsDFTemp['host_count'][i] = response['applications'][i]['application_summary']['host_count']
					if 'concurrent_instance_count' in response['applications'][i]['application_summary']:
						appsDFTemp['concurrent_instance_count'][i] = response['applications'][i]['application_summary']['concurrent_instance_count']
					appsDFTemp['app_apdex_threshold'][i] = response['applications'][i]['settings']['app_apdex_threshold']
					appsDFTemp['end_user_apdex_threshold'][i] = response['applications'][i]['settings']['end_user_apdex_threshold']
					appsDFTemp['enable_real_user_monitoring'][i] = response['applications'][i]['settings']['enable_real_user_monitoring']
					appsDFTemp['use_server_side_config'][i] = response['applications'][i]['settings']['use_server_side_config']
					if len(response['applications'][i]['links']['application_instances']) > 0:
						appsDFTemp['application_instances'][i] = response['applications'][i]['links']['application_instances']
					if len(response['applications'][i]['links']['servers']) > 0:
						appsDFTemp['servers'][i] = response['applications'][i]['links']['servers']
					if len(response['applications'][i]['links']['application_hosts']) > 0:
						appsDFTemp['application_hosts'][i] = response['applications'][i]['links']['application_hosts']
		# Subset appsDFTemp to only include rows where reporting is True
		appsDFTemp = appsDFTemp.loc[appsDFTemp['reporting'] == True]
		appsDF = appsDF.append(appsDFTemp, ignore_index=True)

	# Subset appsDF to only include rows where reporting is True
	appsDF = appsDF.loc[appsDF['reporting'] == True]

	# Subset appsDF to exclude rows where name like 'jive'
	appsDF = appsDF.loc[~appsDF['name'].str.contains('jive')]

	# Subset appsDF to exclude rows where name like 'thunder'
	appsDF = appsDF.loc[~appsDF['name'].str.contains('thunder')]	
	
	# Write data frame to CSV
	with open(appListFileName, 'w') as f:
		appsDF.to_csv(f, sep=',', encoding='utf-8', index=False)
		f.close()

def getAppMetricsData():
	# Read appListFileName into pandas dataframe (appsDF)
	appsDF = pandas.DataFrame.from_csv(appListFileName, header=0, index_col=None)

	print 'Read ' + str(len(appsDF.index)) + ' applications'

	appMetricsColumnNames = ['id','name','from','to','endUserAvgResponseTime','endUserApdexScore','endUserApdex_s_count','endUserApdex_t_count','endUserApdex_f_count','endUserApdex_total_count']

	dateRangeDF = pandas.DataFrame(columns=['StartDate', 'EndDate'])
	SUN = 6
	# startDate = '2016-09-04'
	# start_date = datetime.datetime.strptime(startDate, '%Y-%m-%d').date()
	end_date = date.today()
	start_date = end_date - timedelta(days=7)
	startDate = start_date.strftime('%Y-%m-%d')
	print 'Getting number of Sundays between ' + str(start_date) + ' and ' + str(end_date)    # debug

	days = [date.fromordinal(d) for d in  
				range( start_date.toordinal(),
					   end_date.toordinal()+1 )]
					   
	sundays = [d for d in days if d.weekday() == SUN]

	print 'There are ' + str(len(sundays)) + ' Sundays between ' + str(start_date) + ' and ' + str(end_date)

	for i in range(len(sundays)):
		dateRangeDFTemp = pandas.DataFrame(index=range(1), columns=['StartDate', 'EndDate'])
		dateRangeDFTemp['StartDate'][0] = (datetime.datetime.strptime(startDate, '%Y-%m-%d').date() + datetime.timedelta(days=7*i)) - datetime.timedelta(days=6)
		dateRangeDFTemp['EndDate'][0] = (datetime.datetime.strptime(startDate, '%Y-%m-%d').date() + datetime.timedelta(days=7*i))
		dateRangeDF = dateRangeDF.append(dateRangeDFTemp, ignore_index=True)

	metricNames = ['EndUser/Apdex', 'EndUser']
	metricValues = ['score', 's', 't', 'f', 'count', 'average_response_time']

	# appMetricsDFErrors = pandas.DataFrame(index=range(str(len(sundays))*str(len(appsDF.index))), columns=appMetricsColumnNames)

	for appID in appsDF['id']:
		print 'Getting metrics data for ' + appsDF.loc[appsDF['id'] == appID, 'name'].iloc[0] + ' (app ' + str(appsDF[appsDF['id']==appID].index.tolist()[0]+1) + ' of ' + str(len(appsDF.index)) + ')'
		for i in range(len(dateRangeDF)):
			startDate = datetime.datetime.combine(dateRangeDF['StartDate'][i], datetime.time(0, 0, 0))
			endDate = datetime.datetime.combine(dateRangeDF['EndDate'][i], datetime.time(23, 59, 59))
			try:
				response = app.metric_data(id=appID, names=metricNames, values=metricValues, from_dt=startDate, to_dt=endDate, summarize=True)
			except:
				# Need to add a dataFrame here to collect list of failures, would want to rerun at end of or something
				continue
			appMetricsDFTemp = pandas.DataFrame(index=range(1), columns=appMetricsColumnNames)
			appMetricsDFTemp['id'][0] = appID
			appMetricsDFTemp['name'][0] = appsDF.loc[appsDF['id'] == appID, 'name'].iloc[0]
			if len(response['metric_data']['metrics']) > 0:
				appMetricsDFTemp['from'][0] = \
					response['metric_data']['from']
				appMetricsDFTemp['to'][0] = \
					response['metric_data']['to']
				appMetricsDFTemp['endUserAvgResponseTime'][0] = \
					response['metric_data']['metrics'][0]['timeslices'][0]['values']['average_response_time']
				try:
					appMetricsDFTemp['endUserApdexScore'][0] = response['metric_data']['metrics'][1]['timeslices'][0]['values']['score']
					appMetricsDFTemp['endUserApdex_s_count'][0] = response['metric_data']['metrics'][1]['timeslices'][0]['values']['s']
					appMetricsDFTemp['endUserApdex_t_count'][0] = response['metric_data']['metrics'][1]['timeslices'][0]['values']['t']
					appMetricsDFTemp['endUserApdex_f_count'][0] = response['metric_data']['metrics'][1]['timeslices'][0]['values']['f']
					appMetricsDFTemp['endUserApdex_total_count'][0] = response['metric_data']['metrics'][1]['timeslices'][0]['values']['count']
				except:
					appMetricsDFTemp['endUserApdexScore'][0] = None
					appMetricsDFTemp['endUserApdex_s_count'][0] = None
					appMetricsDFTemp['endUserApdex_t_count'][0] = None
					appMetricsDFTemp['endUserApdex_f_count'][0] = None
					appMetricsDFTemp['endUserApdex_total_count'][0] = None
				if appsDF[appsDF['id']==appID].index.tolist()[0]+1 == 1:
					with open(appMetricsDataFileName, 'w') as g:
						appMetricsDFTemp.to_csv(g, sep=',', encoding='utf-8', index=False)
						g.close()
				else:
					with open(appMetricsDataFileName, 'a') as g:
						appMetricsDFTemp.to_csv(g, sep=',', encoding='utf-8', index=False, header=False)
						g.close()
			else:
				continue