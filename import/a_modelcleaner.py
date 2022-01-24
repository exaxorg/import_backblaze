datasets = ('source', 'previous')

description = "Some model names need fixing."


def prepare(job):
	dw = job.datasetwriter(parent=datasets.source, previous=datasets.previous)
	dw.add('cleanmodel', 'ascii')
	return dw


def analysis(sliceno, prepare_res):
	dw = prepare_res
	for model in datasets.source.iterate(sliceno, 'model'):
		model = model.replace('  ', ' ')  # "WDC  WUH721414ALE6L4"
		model = model.replace(' ', '_')   # let's avoid spaces completely
		dw.write(model)
