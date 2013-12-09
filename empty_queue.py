#!/usr/bin/env python
import optparse
import os
import time
import socket
import datetime
import pyrax
import pyrax.exceptions as exc

REGIONS = ['IAD', 'ORD', 'DFW', 'LON', 'SYD', 'HKG']

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

def connect_to_queue(first_client, region_id, name):
	"""
	Returns the requested queue in the requested region. We pass in the client
	created during authentication, so we don't have to create a duplicate.
	"""

	client = None

	if first_client.region_name == region_id:
		client = first_client
	else:
		client = pyrax.connect_to_queues(region=region_id)

	print "Connection established to the {0} Region".format(client.region_name)

	found_queue = None
	queues = client.list()
	for queue in queues:
		if queue.name == name:
			print "Queue found in Region {0}".format(client.region_name)
			found_queue = queue
			break
	return found_queue


def empty_queue(queue):
	while (True):
		msgs = queue.list(include_claimed=True, echo=True, limit=10)
		if len(msgs) == 0:
			return
		for msg in msgs:
			if msg.claim_id:
				msg.delete(claim_id)
			else:
				msg.delete()

def main():
	p = optparse.OptionParser(description="Cloud Queues Demo Queue Emptier",
							  prog="python empty_queue.py",
							  version="0.1",
							  usage="%prog [options]")

	p.add_option('--region', '-r',
				 default='IAD',
				 help="The Region in which to delete messages")

	p.add_option('--prefix', '-p',
				 default='demo-',
				 help="The prefix that gets prepended to the lower-cased region code for queue names")
	options, arguments = p.parse_args()

	if options.region not in REGIONS:
		p.print_help()
		print "\nSpecified region not one of {0}.  Exiting...".format(REGIONS)
		exit(1)

	pq = authenticate()
	queue = connect_to_queue(pq, options.region, options.prefix+options.region.lower())
	if queue == None:
		print "Cannot find requested queue. exiting."
		exit(1)
	
	empty_queue(queue)
		
if __name__ == '__main__':
	main()

