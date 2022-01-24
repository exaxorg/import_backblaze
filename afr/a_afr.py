from collections import defaultdict, Counter

datasets = ('source',)

description = "Compute days, fails, and unique serial numbers per drive model."


def analysis(sliceno):
	dayspermodel = Counter()
	failspermodel = Counter()
	serialspermodel = defaultdict(set)

	# - The ".iterate()" function will iterate over all data in ONE dataset,
	#   which corresponds to three months of Backblaze data.  (One zip-file.)
	#   For a longer duration, use ".iterate_chain()" and the "range=" option.
	#
	# - "hashlabel=" is used to assert that the dataset is hash partitioned
	#   on the "cleanmodel" column, since we rely on this in the computations.
	#
	# - Hash partitioning on the "cleanmodel" columns means that each model
	#   only exists in one slice.  So we do not need to consider other slices
	#   for the per model statistics.

	for model, serial, failure in datasets.source.iterate(sliceno, ('cleanmodel', 'serial_number', 'failure'), hashlabel='cleanmodel'):
		serialspermodel[model].add(serial)
		dayspermodel[model] += 1
		if failure:
			failspermodel[model] += 1

	# No need to return all serial numbers.  The unique count of
	# how many there are is sufficient

	serialspermodel = {model: len(val) for model, val in serialspermodel.items()}
	return dayspermodel, failspermodel, serialspermodel


def synthesis(analysis_res):
	dayspermodel, failspermodel, serialspermodel = analysis_res.merge_auto()
	return dayspermodel, failspermodel, serialspermodel
