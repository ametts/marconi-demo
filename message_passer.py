#!/usr/bin/env python
import optparse
import os
import time
import socket
import datetime
import pyrax

def authenticate():
	"""
	Reads credentials and authenticates
	"""
	pyrax.set_setting("identity_type", "rackspace")
	creds_file = os.path.expanduser("~/.rackspace_cloud_credentials")
	pyrax.set_credential_file(creds_file)

	# One of several "clients" we'll create (one for each data center).  
	# The first one handles the requisite setup of pyrax.
	# For the atldemo account, this one is probably IAD
	pq = pyrax.queues
	print "Authenticated in region {0}".format(pq.region_name)
	return pq

def connect_to_queues(first_client, source_name, dest_name):
	"""
	Connects to all available regions and finds the queues with the
	passed-in names.  The first match wins. We pass in the client
	created during authentication, so we don't have to create a duplicate.
	
	Returns clients that can access the respective queues
	"""

	REGIONS = ['IAD', 'ORD', 'DFW', 'LON', 'SYD', 'HKG']

	clients = {}

	print "Locating queues in Regions"

	for region_id in REGIONS:
		if first_client.region_name == region_id:
			clients[region_id] = first_client
		else:
			newclient = pyrax.connect_to_queues(region=region_id)
			clients[region_id] = newclient

	print "Connections established to {0} Regions".format(len(clients))

	source_queue = None
	dest_queue = None
	for client in clients.values():
		queues = client.list()
		for queue in queues:
			if queue.name == source_name:
				print "Source queue found in Region {0}".format(client.region_name)
				source_queue = queue
				break
		for queue in queues:
			if queue.name == dest_name:
				print "Destination queue found in Region {0}".format(client.region_name)
				dest_queue = queue
				break
	return source_queue, dest_queue


def check_for_messages(source, destination):
	try:
		claim = source.claim_messages(60, 60, 10)
		if claim is not None and len(claim.messages) > 0:
			print "Claimed {0} messages for forwarding".format(len(claim.messages))
			forwarding_stamp = "{0} {1} Forwarded message to destination\n".format(
				datetime.datetime.utcnow(),
				socket.gethostname(),
				)
			for msg in claim.messages:
				new_body = msg.body + forwarding_stamp
				destination.post_message(new_body,msg.ttl)
				msg.delete(msg.claim_id)
	except pyrax.exceptions.ClientException as e:
		print e.__str__()

def main():
	p = optparse.OptionParser(description="Cloud Queues Demo Message Passer",
							  prog="python message_passer.py",
							  version="0.1",
							  usage="%prog [options]")

	p.add_option('--source', '-s',
				 help="Queue name to read from")

	p.add_option('--destination', '-d',
				 help="Queue name to write to")

	options, arguments = p.parse_args()

	if not options.source:
		p.print_help()
		print "\nSource queue not specified.  Exiting..."
		exit(1)
	if not options.destination:
		p.print_help()
		print "\nDestination queue not specified.  Exiting..."
		exit(1)

	pq = authenticate()

#	msgs = pq.list_messages(options.source, echo=True, include_claimed=True)
#	for msg in msgs:
#		print "{0} | {1} | {2} | {3}".format(msg.id, msg.age, msg.ttl, msg.claim_id) 

	source_queue, dest_queue = connect_to_queues(pq, options.source, options.destination)
	if source_queue == None or dest_queue == None:
		print "Cannot find source and/or destination queue, exiting."
		exit(1)

	# Check for messages once each second.  Function will read and forward messages as 
	# fast as possible once one is found (until the queue is empty again)
	while(True):
		check_for_messages(source_queue, dest_queue)
		time.sleep(1)
		
if __name__ == '__main__':
	main()

