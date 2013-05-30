#!/usr/bin/python

# csv_lookup.py will add reverse dns information to a any supplied csv file.
# provide an input file, output file, and the column that contains the IPs to be looked up # prior to dooing the lookups, csv_lookup.py will grab a list of distince IPs to lessen required resouces # csv_lookup.py will add a column to the right of the IP column ad insert reverse DNS information.
# rob22202@gmail.com, rob22202 on github

import time, sqlite3, socket, csv, sys, getopt, urllib2, re

if hasattr(socket, 'setdefaulttimeout'):
  socket.setdefaulttimeout(2)

def main(argv):
	start_time = time.time()
	inputfile = ""
	outputfile = ""
	verbose = "no"
	usage = "Usage: csv_lookup.py -i <inputfile> -o <outputfile> -c <mumber of column with IPs>\r\nNote: -c Should be a numerical value that denotes the column contining IP addresses to be resolved, starting from the left and counting the first column as zero."
	try:
		opts, args = getopt.getopt(argv,"hi:o:c:",["ifile=","ofile=","col="])
	except getopt.GetoptError:
		print usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
				print usage
				sys.exit()
		elif opt in ("-i", "--ifile"):
				inputfile = arg
		elif opt in ("-o", "--ofile"):
				outputfile = arg
		elif opt in ("-c", "--col"):
				ip_column = int(arg)
	if inputfile == "" or outputfile == "" or ip_column =="":
		print usage
		sys.exit(2)
	print ""
	print "Looking up ip addresses in: " + inputfile
	print "Writing reults to: " + outputfile
	conn = sqlite3.connect("csv_lookup.db")
	curs = conn.cursor()
	curs.execute("DROP TABLE IF EXISTS ips;")
	curs.execute("CREATE TABLE ips (ip text);")
	curs.execute("DROP TABLE IF EXISTS ip_info;")
 	curs.execute("CREATE TABLE ip_info (ip text, reverse text);")
	csvreader = csv.reader(open(inputfile, 'r'))
	rec_count = 0
	for row in csvreader:
		rec_count = rec_count + 1
		dip = row[int(ip_column)]
			to_db = [unicode(dip, "utf8")]
		curs.execute("INSERT INTO ips (ip) VALUES (?);", to_db)
	conn.commit()
	curs.execute("select distinct ip from ips;")
	distinct_ips = curs.fetchall()
	ip_count = len(distinct_ips)
	mod = ip_count / 20
	print "CSV contained " + str(rec_count) + " flows and " + str(ip_count) + " distinct destination IP Addresses"
	print ""
	print "Looking up " + str(ip_count) + " DNS records"
	i = 0
	percent = 0
	for row in distinct_ips :
		i = i + 1
		if i%mod == 0:
			percent = percent + 5
			if percent < 96:
				print "- " + str(percent) + "% Complete. " + str(i) + " lookups done."
		ip = str(row[0])
		try:
			reverse = str(reverse_dns(row[0]))
			category = "None"
		except:
			pass
		if verbose == "yes":
			print ip + " -> " + reverse
		curs.execute ("INSERT INTO ip_info (ip, reverse) VALUES (?, ?);", (ip,reverse))	
	conn.commit()
	print "- 100% Complete. " + str(ip_count) + " lookups done."
	print ""
	print "Writing " + str(rec_count) + " records to " + outputfile
	mod = rec_count / 20
	i = 0
	percent = 0
	with open(inputfile, 'rb') as input_csv:
		with open (outputfile, 'wb') as output_csv:
			reader = csv.reader(input_csv)
			writer = csv.writer(output_csv)
			for row in reader:
				new_row = row	
				i = i + 1
                		if i%mod == 0:
					percent = percent + 5
					if percent < 96:
						print "- " + str(percent) + "% Complete. " + str(i) + " rows written."
				dip = row[ip_column]
				curs.execute("select reverse from ip_info where ip = ?;", [dip])
				results = curs.fetchone()
				if results:
					dns = results[0]
				else:
					dns = "No Data"
				new_row.append("")
				c = 0
				for column in row:
					shove = ip_column + 1
					if c > shove:
						new_row[shove] = row[c]
					c = c + 1
				new_row[ip_column + 1] = results[0]
				writer.writerow(new_row)
		output_csv.close()
	print "- 100% complete. " + str(rec_count) + " rows written."
	print ""
	end_time = time.time()
	print("Elapsed time was %g seconds" % (end_time - start_time))
	print ""

def reverse_dns(ipInput):
	try:
		reverse_info = socket.gethostbyaddr(ipInput)
		return reverse_info[0]
	except:
		return "None"

if __name__ == "__main__":
	main(sys.argv[1:])
