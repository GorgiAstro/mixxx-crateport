#!/usr/bin/env python
#

import sqlite3
import os
import sys
import xml.dom
import xml.dom.minidom
import platform
import fileinput
import tarfile
from optparse import OptionParser


def generateCrateXML(crates):
	dom = xml.dom.getDOMImplementation()
	document = dom.createDocument(None, None, None)
	ncrates = document.createElement('crates')
	document.appendChild(ncrates)
	
	for cratename in crates:
		ncrate = document.createElement('crate')
		ncrates.appendChild(ncrate)
		ncrate.setAttribute('name', cratename)
		
		for track in crates[cratename]:
			ntrack = document.createElement('track')
			ncrate.appendChild(ntrack)
			for key in list(track.keys()):
				ntrack.setAttribute(key, str(track[key]))
	
	return document.toprettyxml()

def listCrates(conn):
	cursor = conn.cursor()
	cratelist = []
	
	cursor.execute("SELECT id, name FROM crates")
	
	row = cursor.fetchone()
	while row:
		cratelist.append(row[1])

		
		row = cursor.fetchone()
	
	return cratelist

def getCrates(conn):
	cursor = conn.cursor()
	crates = {}
	
	cursor.execute("SELECT id, name FROM crates")
	
	row = cursor.fetchone()
	while row:
		crates[row['name']] = []
		
		cur2 = conn.cursor()
		cur2.execute("""
			SELECT
				library.artist AS artist,
				library.title AS title,
				track_locations.location,
				track_locations.filename
			
			FROM crate_tracks
				INNER JOIN library
					ON crate_tracks.track_id = library.id
				INNER JOIN track_locations
					ON library.location = track_locations.id
			WHERE
				crate_tracks.crate_id = ?
			
			""", (str(row['id']),))
		
		track = cur2.fetchone()
		
		while track:
			crates[row['name']].append(track)
			track = cur2.fetchone()
		
		row = cursor.fetchone()
	
	return crates

# just the filenames, so we can generate a tar file.
def filenamesfromCrates(conn):
	cursor = conn.cursor()
	crates = {}
	files = []
	
	cursor.execute("SELECT id, name FROM crates")
	
	row = cursor.fetchone()
	while row:
		crates[row['name']] = []
		
		cur2 = conn.cursor()
		cur2.execute("""
			SELECT
				track_locations.location,
				track_locations.filename
			
			FROM crate_tracks
				INNER JOIN library
					ON crate_tracks.track_id = library.id
				INNER JOIN track_locations
					ON library.location = track_locations.id
			WHERE
				crate_tracks.crate_id = ?
			
			""", (str(row['id']),))
		
		track = cur2.fetchone()
		
		while track:
			files.append(track[0])
			track = cur2.fetchone()
		
		row = cursor.fetchone()
	
	return files

def findTrack(conn, ntrack):
	location = ntrack.getAttribute('location')
	artist = ntrack.getAttribute('artist')
	title = ntrack.getAttribute('title')
	filename = ntrack.getAttribute('filename')
	
	cursor = conn.cursor()
	
	cursor.execute("""
		SELECT
			l.id,
			l.filetype
			FROM library l
			INNER JOIN track_locations tl
				ON l.location = tl.id
			WHERE 
				(tl.location = ?)
		""", (location,))
	
	track = cursor.fetchone()
	if track != None:
		return track
	
	cursor.execute("""
		SELECT
			l.id,
			l.filetype
			FROM library l
			INNER JOIN track_locations tl
				ON l.location = tl.id
			WHERE 
				(tl.filename = ?)
		""", (filename,))
	
	track = cursor.fetchone()
	if track != None:
		return track
	
	cursor.execute("""
		SELECT
			l.id,
			l.filetype
			FROM library l
			WHERE 
				(l.artist = ? AND l.title = ?)
		""", (artist, title))
	
	track = cursor.fetchone()
	if track != None:
		return track
	
	return None

def importCrateXML(conn, dcrate):
	cursor = conn.cursor()
	ncrates = dcrate.documentElement
	if ncrates.tagName != 'crates':
		raise Exception('Not a Crates XML File')
	
	for ncrate in ncrates.childNodes:
		if ncrate.tagName != 'crate':
			raise Exception('Not a Crate')
		
		try:
			cursor.execute("INSERT INTO crates(name) VALUES(?)", 
				(ncrate.getAttribute('name'),))
			print("Creating new Crate:", ncrate.getAttribute('name'))
		except sqlite3.IntegrityError:
			print("Already Created:", ncrate.getAttribute('name'))
		
		cursor.execute("SELECT id FROM crates WHERE name = ?", 
			(ncrate.getAttribute('name'),))
		crate = cursor.fetchone()
		
		for ntrack in ncrate.childNodes:
			if ncrate.tagName != 'crate':
				raise Exception('Not a Crate')
			
			track = findTrack(conn, ntrack)
			if track != None:
				try:
					print("Adding a Track")
					cursor.execute("""
						INSERT INTO crate_tracks(crate_id, track_id)
						VALUES(?, ?)
					""", (str(crate['id']), track['id']))
				except sqlite3.IntegrityError:
					print("Track already in crate")

def main():
	home = os.path.expanduser('~')
	uname = platform.uname()
	if uname[0] == 'Darwin':
		cfgdir = home + '/Library/Application Support/Mixxx'
	elif uname[0] == 'Linux':
		cfgdir = home + '/.mixxx'
	
	defdb = cfgdir + '/mixxxdb.sqlite'	

	opt = OptionParser(description='Import and Export Crates from Mixxx')
	opt.add_option('-i', '--import', dest='export', action='store_false')
	opt.add_option('-e', '--export', dest='export', action='store_true')
	opt.add_option('-d', '--dbname', dest='dbname', default=defdb)
	opt.add_option('-l', '--list', dest='listcrates', action='store_true', default=False)
	opt.add_option('-t', '--tar', dest='tarcrates', action='store_true', default=False)
	
	(options, args) = opt.parse_args()

	if options.export == None: options.export = True;	
	conn = sqlite3.connect(options.dbname)
	conn.row_factory = sqlite3.Row

	if options.listcrates == True:
		print("list of crates:")
		for cratename in listCrates(conn):
			print(cratename)
		sys.exit(0)
	# simple streaming tar to stdout... dont send anything else to stdout if we do this, as it will mess up the tar file
	if options.tarcrates == True:
		tar = tarfile.open(fileobj=sys.stdout,mode='w|')
		# list set stuff to make list unique to get rid of doubled up file names.
		for filename in list(set(filenamesfromCrates(conn))):
			tar.add(filename)
		tar.close()
		sys.exit(0)
	
	if options.export == True:
		output = open(args[0], "w")  if len(args) > 0 else sys.stdout
		crates = getCrates(conn)
		output.write(generateCrateXML(crates))
	else:
		input = open(args[0], "r") if len(args) > 0 else sys.stdin
		crates = xml.dom.minidom.parse(input)
		importCrateXML(conn, crates)
	
	conn.commit()
	conn.close()

if __name__ == '__main__':
	main()
