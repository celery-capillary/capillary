from massimport.celery import app
from collections import Mapping
from celery import group


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
def dict_reducer(self, items, extra=None):
    """Combine a series of dictionaries into a single dict

    :param items: Recursive structure containing Lists of dictionaries or lists of the recursive structure

    :returns dict: Single combined dictionary
    """

    print 'dict_reducer: {}'.format(items)
    if extra:
        print 'dict_reducer: got some extra: {}'.format(extra)

    # if items is a mapping, just return it
    if isinstance(items, Mapping):
        return items

    res = {}
    for i in items:
        # If i is dict-like, update the result
        if isinstance(i, Mapping):
            # TODO: list values should .extend, not clobber
            res.update(i)
        else:
            # Aught to be a list, recurse
            res.update(dict_reducer(i))
    return res


# TODO - this is a bit hinky, but solves the kickoff problem
@app.task(bind=True)
def concat(self, acc, arg=None, *args):
    """Just return the arg appended to the accumulator.

    One positional should be the arg.
    Two positional arguments should be accumulator, arg

    :param arg: object to append to a list
    :param acc: optional list to append to, if missing new list will be created
        if not a list, it will be wrapped with a list.
    """
    print 'concat call({}, {}, {})'.format(acc, arg, args)
    if arg is None:
        arg = acc
        acc = []

    print 'concat fixed({}, {}, {})'.format(acc, arg, args)

    if not isinstance(acc, list):
        # Support upgrading a single item to a list
        acc = [acc]

    print 'concat action({}, {})'.format(acc, arg)

    return acc + [arg]


@app.task(bind=True)
def generator(self, arg, *args, **kwargs):
    """Just return the first arg as the results. Ignores any other params"""
    print 'generator: {}'.format(arg)
    return arg


@app.task(bind=True)
def lazy_async_apply_map(self, items, d, runner):
    """use mapper to extract items from d, process them with runner, then
    reduce the results with reducer

    :param items: itterable of arguments for the runner
    :param d: data to operate on (probably returned by a previous task)
    :param runner: task signature to execute on each item. def runner(item, data, *a, **kw)
    """

    subtasks = []
    for item in items:
        r = runner.clone()
        r.args = (item, d) + r.args
        subtasks.append(r)

    try:
        # Celery master
        raise self.replace(group(*subtasks))
    except AttributeError:
        # Celery 3.1
        raise self.replace_in_chord(group(*subtasks))


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
