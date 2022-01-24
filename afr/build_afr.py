from accelerator.colour import bold, faint, red, brightblue as blue

description = "Calculate Annual Failure Rates (AFR)."


def main(urd):
	try:
		imp = urd.peek('import_hash_cleanmodel_sort', '2021-03-31').joblist[-1]
	except IndexError:
		print(red('You need to run the import first'))
		return 1
	job = urd.build('afr', source=imp)
	dayspermodel, failspermodel, serialspermodel = job.load()

	afr = {}
	for model, days in dayspermodel.items():
		if serialspermodel[model] >= 60:
			afr[model] = 365 * failspermodel[model] / days

	print()
	col = imp.dataset().columns['date']
	period = "%s through %s inclusive" % (col.min, col.max,)
	print(blue("Reporting period: %s" % (bold(period,))))
	print(bold("model                 #drives    #days   #fails       AFR"))
	print("---------------------------------------------------------")
	afr = sorted(afr.items(), key=lambda x: -x[1])
	for ix, (model, val) in enumerate(afr):
		line = "%-20s %8d %8d %8d %8.2f%%" % (
			model[:20],
			serialspermodel[model],
			dayspermodel[model],
			failspermodel[model],
			100 * val,
		)
		if ix % 2:
			line = faint(line)
		print(line)
	print("---------------------------------------------------------")
