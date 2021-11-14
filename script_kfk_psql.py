import requests, datetime, json, os, psycopg2

#from config.kafka_config import *

cert_folder='/app/cert_folder'
hostname='kafka-13d0a2b2-blogremindme-cdf4.aivencloud.com'
port=11990

def sendMsg(message):

	# This script connects to Kafka and send a few messages

	from kafka import KafkaProducer

	#create Producer with kafka instance created in aiven.io
	producer = KafkaProducer(
	    bootstrap_servers=hostname+":"+str(port),
	    security_protocol="SSL",
	    ssl_cafile=cert_folder+"/ca.pem",
	    ssl_certfile=cert_folder+"/service.cert",
	    ssl_keyfile=cert_folder+"/service.key",
	)

	#for i in range(1, 4):
	#    message = "message number {}".format(i)
	print(f"Sending: {message}")
	producer.send("demo-topic", message.encode("utf-8"))

	# Force sending of all messages

	producer.flush()

POSTGRESQL_URI='postgres://avnadmin:Lk7YXjh4FTVhgF8d@pg-2a999da2-blogremindme-cdf4.aivencloud.com:11988/defaultdb?sslmode=require'

def psqlCon(query_sql):
    conn = psycopg2.connect(POSTGRESQL_URI)

    #query_sql = 'SELECT VERSION()'

    cur = conn.cursor()
    cur.execute(query_sql)

    #version = cur.fetchone()[0]
    #print(version)

def readMsgs():

	# This script receives messages from a Kafka topic

	from kafka import KafkaConsumer

	consumer = KafkaConsumer(
	    "demo-topic",
	    auto_offset_reset="earliest",
	    bootstrap_servers=hostname+":"+str(port),
	    client_id="demo-client-1",
	    group_id="demo-group",
	    security_protocol="SSL",
	    ssl_cafile=cert_folder+"/ca.pem",
	    ssl_certfile=cert_folder+"/service.cert",
	    ssl_keyfile=cert_folder+"/service.key",
	)

	# Call poll twice. First call will just assign partitions for our
	# consumer without actually returning anything

	for _ in range(2):
	    raw_msgs = consumer.poll(timeout_ms=1000)
	    for tp, msgs in raw_msgs.items():
	        for msg in msgs:
	            print("Received: {}".format(msg.value))
	            query_sql=''
	            #psqlCon(query_sql)

	# Commit offsets so we won't get the same messages again
	#consumer.commit()


urls = ['http://google.com', 'http://colleges-in-canada.org', 'http://vgoogle.com', 'http://kehellandhort.co.uk']

for url in urls:
	
	#print(url)

	time = datetime.datetime.now()
	#print(time)

	try:
		response = requests.get(url, timeout=3)
		#check if web is available or not with status_code 4xx or 5xx
		sc = response.status_code
		#print(sc)
	except Exception as e:
		message=str({'url':url, 'time':time, 'status':'timeout'})
		sendMsg(message)
		continue

	message=str({'url':url, 'time':time, 'status':sc})

	sendMsg(message)

readMsgs()



