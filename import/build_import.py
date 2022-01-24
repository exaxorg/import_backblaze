description = """Import backblaze data into dataset chains.  Hash partition on serial number/date/model."""

from os.path import exists, join
from accelerator.colour import bold, red, brightblue as blue

def main(urd):
	urd.set_workdir('import')

	files = [
		# Download these files from
		# https://www.backblaze.com/b2/hard-drive-test-data.html
		# ('data_2013.zip', '2013-12-31'),
		# ('data_2014.zip', '2014-12-31'),
		# ('data_2015.zip', '2015-12-31'),
		# ('data_Q1_2016.zip', '2016-03-31'),
		# ('data_Q2_2016.zip', '2016-06-30'),
		# ('data_Q3_2016.zip', '2016-09-30'),
		# ('data_Q4_2016.zip', '2016-12-31'),
		# ('data_Q1_2017.zip', '2017-03-31'),
		# ('data_Q2_2017.zip', '2017-06-30'),
		# ('data_Q3_2017.zip', '2017-09-30'),
		# ('data_Q4_2017.zip', '2017-12-31'),
		# ('data_Q1_2018.zip', '2018-03-31'),
		# ('data_Q2_2018.zip', '2018-06-30'),
		# ('data_Q3_2018.zip', '2018-09-30'),
		# ('data_Q4_2018.zip', '2018-12-31'),
		# ('data_Q1_2019.zip', '2019-03-31'),
		# ('data_Q2_2019.zip', '2019-06-30'),
		# ('data_Q3_2019.zip', '2019-09-30'),
		# ('data_Q4_2019.zip', '2019-12-31'),
		# ('data_Q1_2020.zip', '2020-03-31'),
		# ('data_Q2_2020.zip', '2020-06-30'),
		# ('data_Q3_2020.zip', '2020-09-30'),
		# ('data_Q4_2020.zip', '2020-12-31'),
		('data_Q1_2021.zip', '2021-03-31'),   # Only this file is needed for AFR-calculation
		# ('data_Q2_2021.zip', '2021-06-30'),
		# ('data_Q3_2021.zip', '2021-09-30'),
	]

	# Typing information for columns present in all files
	column2type_all = dict(
		capacity_bytes='int64_10',
		date='date:%Y-%m-%d',
		failure='strbool',
		model='ascii',
		serial_number='ascii',
	)


	# import and type

	print(bold(blue('Import and type')))
	urdkey_import = 'import_type'
	# In a production project you would not truncate the urd db, but
	# we do it here to simplify if you donwload older files later.
	urd.truncate(urdkey_import, 0)
	last_imported = urd.peek_latest(urdkey_import).timestamp
	for file, date in files:
		if not exists(join(urd.info.input_directory, file)):
			print(red('file "%s" does not exist!' % (file,)))
			continue
		print(file)
		if date > last_imported:
			urd.begin(urdkey_import, date, caption=file)
			prev = urd.latest(urdkey_import).joblist
			job = urd.build('csvimport_zip',
							filename=file,
							# The zips have some extra Mac gunk in them which we don't want
							exclude_re=r'(__MACOSX|\.DS_Store)',
							chaining='by_filename',
							strip_dirs=True,
							previous=prev.get('csvimport_zip'),
			)
			# There are more columns in newer input files.
			# Set types and defaults to match the data.
			columns = job.dataset().columns
			column2type = {}
			defaults = {}
			for col in columns:
				if col in column2type_all:
					column2type[col] = column2type_all[col]
				else:
					column2type[col] = 'int32_10' if 'normalized' in col else 'int64_10'
					defaults[col] = None
			# type and hashpart
			job = urd.build('dataset_type', source=job, column2type=column2type, defaults=defaults, previous=prev.get('dataset_type'))
			job = urd.build('modelcleaner', source=job, previous=prev.get('modelcleaner'))
			urd.finish(urdkey_import)

	if not urd.peek_latest(urdkey_import):
		print(red("Nothing imported, have you donwloaded the files to %r?" % (urd.info.input_directory,)))
		return 1

	# hashpart filtered dataset on serial number + sort

	print(bold(blue('Hash partition')))
	for hashcol in ('serial_number', 'date', 'cleanmodel'):
		print('Hash partition on "%s".' % (hashcol,))
		urdkey_hash = 'import_hash_%s_sort' % (hashcol,)
		for ts in urd.since(urdkey_import, 0):
			if urd.peek(urdkey_hash, ts):
				# This is already done.
				continue
			urd.begin(urdkey_hash, ts)
			impitem = urd.get(urdkey_import, ts)
			prev = urd.latest(urdkey_hash).joblist
			imp = impitem.joblist[-1]
			print(impitem.caption)
			imp = urd.build('dataset_filter_columns', keep_columns=('date', 'cleanmodel', 'serial_number', 'failure'), source=imp, previous=prev.get('dataset_filter_columns'))
			imp = urd.build('dataset_hashpart', hashlabel=hashcol, source=imp, previous=prev.get('dataset_hashpart'))
			imp = urd.build('dataset_sort', sort_columns='date', source=imp, previous=prev.get('dataset_sort'))
			urd.finish(urdkey_hash)

	print()
	print(bold(blue('Example Shell Commands')))
	print(blue('  To see what is stored in the Urd database, type'))
	print(bold('    ax urd'))
	print(blue('  To see all timestamps in the %s list, type' % (urdkey_import,)))
	print(bold('    ax urd %s/since/0' % (urdkey_import,)))
	print(blue('  To see the entry at timestamp %s in the %s list, type' % (urd.peek_first(urdkey_import).timestamp, urdkey_import,)))
	print(bold('    ax urd %s/%s' % (urdkey_import, urd.peek_first(urdkey_import).timestamp,)))
	print(blue('  To see the latest entry in the %s list, type' % (urdkey_import,)))
	print(bold('    ax urd %s' % (urdkey_import,)))
	print(blue('  To see the latest dataset in the %s list, type' % (urdkey_import,)))
	print(bold('    ax ds :%s:' % (urdkey_import,)))
	print(blue('  To see the first line in the latest dataset in JSON format, type'))
	print(bold('    ax cat :%s: -f json | head -1' % (urdkey_import,)))
