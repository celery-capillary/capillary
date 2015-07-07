from massimport.celery import app


@app.task(bind=True)
def list_to_set_reducer(self, groups):
    """Flatten nested lists to a set of items

    Expected shape of input, List of group results, each item in
    group results is list of items to reduce.
    [[[item1, item2], [item2, item3]], [[item4, item5]]]

    :param groups: List of group results. Each group result is expected to be
    an itterable containing itterables of set members.

    :returns: List of unique values from the input
    :rtype: list
    """
    # TODO does this assume too much knowledge of the shape of the input?
    print 'list_to_set_reducer: {}'.format(groups)
    s = set()
    for g in groups:
        for i in g:
            s.update(i)

    return list(s)


@app.task(bind=True)
def dict_reducer(self, items):
    """Combine a serice of dictonaries into a single dict

    :param *items: List of dictionaries

    :returns dict: Single combined dictionary
    """

    print 'dict_reducer: {}'.format(items)

    # TODO: list values should .extend, not clobber
    res = {}
    for i in items:
        if i:
            res.update(i)
    return res


@app.task(bind=True)
def generator(self, arg):
    """Just return the arg as the results."""
    print 'generate: {}'.format(arg)
    return arg


# TODO - this is a bit hinky, but solves the kickoff problem
@app.task(bind=True)
def concat(self, acc, arg=None, ):
    """Just return the arg appened to the accumulator.

    One positional should be the arg.
    Two positionsals should be accumulator, arg

    :param arg: object to append to a list
    :param acc: optional list to appened to, if missing new list will be created"""
    if arg is None:
        arg = acc
        acc = []
    print 'concat({}, {})'.format(acc, arg)
    return acc + [arg]


@app.task(bind=True)
def serial_runner(self, tasks, task_args=None, task_kwargs=None, reducer=None):
    """A tasks that runs many subtasks syncronously, for batching up lots of small tasks.

    :param tasks: list of tasks to execute
    :param task_args: list of *args for each task
    :param task_kwargs: dict of **kwargs for each task
    :parma reducer: optional task to reduce the results with

    :returns: list or results, or results of reducer
    """
    task_args = task_args or []
    task_kwargs = task_kwargs or {}

    results = []
    for t in tasks:
        results.append(t.apply(args=task_args, kwargs=task_kwargs).result)

    if reducer:
        # List to reduce is first positional arg to task
        results = reducer.apply(args=(results,)).result

    return results
