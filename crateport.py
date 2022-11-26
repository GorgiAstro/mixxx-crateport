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
import shutil
import string


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

def export_separate_m3u_files(crates, write_rel_path=False):
	input("This will overwrite crate files if a M3U file already has the same name, are you sure? Press Ctrl+C to cancel")
	for cratename, tracks in crates.items():
		with open(f'{cratename}.m3u', 'w') as m3u_out:
			m3u_out.write("#EXTM3U\n")
			for track in tracks:
				m3u_out.write("#EXTINF\n")
				song_path = track['location']
				if write_rel_path:
					song_path = os.path.relpath(song_path)
				m3u_out.write(song_path + "\n")


valid_chars = "-_.()'[]&éèàöäüßâù %s%s" % (string.ascii_letters, string.digits)

def export_files_to_folder(crates, out_folder, verbose=False):
	if not os.path.isdir(out_folder):
		raise Exception(f'Specified folder {out_folder} does not exist, exiting')
	
	input("This can copy gigabytes of data, are you sure?! Press Ctrl+C to cancel")

	for cratename, tracks in crates.items():
		crate_out_folder = os.path.join(out_folder, cratename)
		if not os.path.isdir(crate_out_folder):
			print(f'Output folder for crate {cratename} not existing yet, creating it to {crate_out_folder}')
			os.mkdir(crate_out_folder)

		for track in tracks:
			track_path_in = track['location']
			track_extension = os.path.splitext(track['filename'])[1]
			track_name_pretty = f"{track['artist']} - {track['title']}{track_extension}"
			track_name_pretty = ''.join(c for c in track_name_pretty if c in valid_chars)

			track_path_out = os.path.join(crate_out_folder, track_name_pretty)
			if os.path.exists(track_path_out):
				if verbose:
					print(f'Skipping already existing file {track_path_out}')
			else:
				if verbose:
					print(f'Creating file {track_path_out}')
				shutil.copy(track_path_in, track_path_out)

def main():
	home = os.path.expanduser('~')
	uname = platform.uname()
	if uname[0] == 'Darwin':
		cfgdir = home + '/Library/Application Support/Mixxx'
	elif uname[0] == 'Linux':
		cfgdir = home + '/.mixxx'
	
	defdb = cfgdir + '/mixxxdb.sqlite'	

	opt = OptionParser(description='Import and Export Crates from Mixxx')
	opt.add_option('-i', '--import', dest='importt', action='store_true', default=False)
	opt.add_option('-e', '--export', dest='export', action='store_true', default=False)
	opt.add_option('-d', '--dbname', dest='dbname', default=defdb)
	opt.add_option('-l', '--list', dest='listcrates', action='store_true', default=False)
	opt.add_option('-t', '--tar', dest='tarcrates', action='store_true', default=False)
	opt.add_option('-m', '--m3u', dest='export_separate_m3u_files', action='store_true', default=False)
	opt.add_option('-r', '--relativepath', dest='relative_path_for_m3u', action='store_true', default=False)
	opt.add_option('-f', '--exportfilestofolder', dest='export_files_to_folder')
	opt.add_option('-v', '--verbose', dest='verbose', action='store_true', default=False)
	
	(options, args) = opt.parse_args()

	conn = sqlite3.connect(options.dbname)
	conn.row_factory = sqlite3.Row

	if options.listcrates:
		print("list of crates:")
		for cratename in listCrates(conn):
			print(cratename)
		sys.exit(0)
	# simple streaming tar to stdout... dont send anything else to stdout if we do this, as it will mess up the tar file
	elif options.tarcrates:
		tar = tarfile.open(fileobj=sys.stdout,mode='w|')
		# list set stuff to make list unique to get rid of doubled up file names.
		for filename in list(set(filenamesfromCrates(conn))):
			tar.add(filename)
		tar.close()
		sys.exit(0)	
	elif options.export:
		output = open(args[0], "w")  if len(args) > 0 else sys.stdout
		crates = getCrates(conn)
		output.write(generateCrateXML(crates))
	elif options.importt:
		input = open(args[0], "r") if len(args) > 0 else sys.stdin
		crates = xml.dom.minidom.parse(input)
		importCrateXML(conn, crates)
	elif options.export_separate_m3u_files:
		crates = getCrates(conn)
		export_separate_m3u_files(crates, options.relative_path_for_m3u)
	elif options.export_files_to_folder is not None:
		crates = getCrates(conn)
		export_files_to_folder(crates, options.export_files_to_folder, options.verbose)
	else:
		print('No valid option selected, closing')
	
	conn.commit()
	conn.close()

if __name__ == '__main__':
	main()
