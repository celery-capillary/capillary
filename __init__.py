import re
from functools import partial
from collections import defaultdict

import venusian
from structlog import get_logger

# from .tasks import serial_runner
from celery import chain, chord, group, maybe_signature
from celery.exceptions import Ignore
import networkx as nx

logger = get_logger()


class _sentinel(object):
    """Sentinel to identify non-tagged tasks"""


class ALL(object):
    """Constant to note a pipeline element that is ran the last by specifying after=ALL"""


class DependencyError(Exception):
    """Raised when there are problems in the shape of the pipeline"""


class AbortPipeline(Exception):
    """Raised if the pipeline should be aborted all together."""


def pipeline(**kwargs):
    """
    Decorator for configuring pipeline flow declaratively.

    Applying :func:`pipeline` decorator to a function has very little side effects
    once decorator is evaluated. :func:`pipeline decorator attaches :attr:`callback`
    attribute to the function that only later when :class:`PipelineConfigurator`
    is initialized preforms following actions:

    * wraps the decorator function to become a celery task (with `bind=True` passed
    by default, see http://celery.readthedocs.org/en/latest/userguide/tasks.html#task-request-info)
    * registers pipeline element and populates :attr:`PipelineConfigurator.registry`

    :param name string: Unique identifier of the pipeline element (defaults to the decorated function name)
    :param tags list: A list of tags this pipeline element belongs to. See
                      :meth:`PipelineConfigurator.run_pipeline` to understand how
                      tags affect what pipeline elements belong together)
    :param error_handling_strategy string: (only for is_parallel)
    :param is_parallel boolean: Should the pipeline element be ran as a separate Celery task in parallel?
    :param after list/string: On what pipeline output does this element depend on. Using constant ALL
                              makes the pipeline element the last one in the chain
    :param mapper string/function: (only for is_parallel) TODO
    :param reducer string/function: (only for is_parallel) TODO
    :param requires_parameter list/string: Names of parameters that will be passed as keyword arguments
                                           to this pipeline element
    :param celery_task_kwargs dict: Keyword arguments passed to the celery task. For a list of options
                                    see http://celery.readthedocs.org/en/latest/userguide/tasks.html#list-of-options
    :raises: :exc:`ValueError` if unknown keyword argument is recieved

    Example::

    >>> @pipeline(
    >>>     after="some_other_pipeline_element",
    >>> )
    >>> def foobar(celery_task, *args):
    >>>     raise NotImplemented

    """

    new_name = kwargs.pop('name', None)
    tags = kwargs.pop('tags', [])
    error_handling_strategy = kwargs.pop('error_handling_strategy', None)
    is_parallel = kwargs.pop('is_parallel', False)
    after = kwargs.pop('after', [])
    mapper = kwargs.pop('mapper', None)
    reducer = kwargs.pop('reducer', None)
    requires_parameter = kwargs.pop('requires_parameter', [])
    celery_task_kwargs = kwargs.pop('celery_task_kwargs', {})

    if isinstance(after, basestring):
        after = [after]
    if isinstance(requires_parameter, basestring):
        requires_parameter = [requires_parameter]

    if kwargs:
        raise ValueError('@pipeline got unknown keyword parameters: {}'.format(kwargs))

    def decorator(wrapped):
        def callback(scanner, name, func):
            name = new_name or name

            # make the function also a celery task
            func = scanner.celery_app.task(
                name=func.__module__ + '.' + name,
                bind=True,
                **celery_task_kwargs
            )(func)

            info = {
                'func': func,
                'name': name,
                'error_handling_strategy': error_handling_strategy,
                'is_parallel': is_parallel,
                'after': after,
                'mapper': mapper,
                'reducer': reducer,
                'requires_parameter': requires_parameter,
            }
            logger.debug(
                '@pipeline registered',
                name=name,
                info=info,
                tags=tags,
            )
            if tags:
                for tag in tags:
                    tagged = scanner.registry[tag]
                    if name in tagged:
                        raise ValueError('"{}" pipeline already exists for tag "{}"'.format(name, tag))
                    tagged[name] = info
            else:
                untagged = scanner.registry[_sentinel]
                if name in untagged:
                    raise ValueError('{} pipeline already exists without tags'.format(name))
                untagged[name] = info

        venusian.attach(wrapped, callback, 'pipeline')
        wrapped.callback = callback  # for testing purposes
        return wrapped
    return decorator


def make_pipeline_from_defaults(**kw):
    """
    Prepopulates default keyword parameters to :func:`pipeline` and
    returns a new decorator. Intended to eliminate commonly repeated
    arguments passed to multiple pipeline elements.

    Example::

    >>> foobar_pipeline = make_pipeline_from_defaults(
    >>>     tags=["foobar"]
    >>> )

    >>> @foobar_pipeline(
    >>>     after="some_other_pipeline_element",
    >>> )
    >>> def foobar(self, *args):
    >>>     raise NotImplemented

    """
    return partial(pipeline, **kw)


class PipelineConfigurator(object):
    """Interface for configuring and running the pipeline imperatively.

    TODO: mention what __init__ performs

    Supports mixing sync and async task within a pipeline.

    :param celery_app: Celery app being used as the base.
    :param package: A module to scan for :func:`pipeline` decorators using
                    :meth:`venusian.Scanner.scan`.

    """

    def __init__(self, celery_app, package):
        self.celery_app = celery_app
        self.error_handling_strateies = {}
        self.mappers = {}
        self.reducers = {}

        scanner = venusian.Scanner(registry=defaultdict(dict), celery_app=celery_app)
        scanner.scan(package, categories=['pipeline'], ignore=[re.compile('tests$').search])
        self.registry = scanner.registry

    def add_error_handling_strategy(self, name, callback):
        """
        Configures error handling strategy for is_parallel tasks.
        """
        if name in self.error_handling_strateies:
            raise ValueError('{} error handling strategy already registered'.format(name))
        self.error_handling_strateies[name] = callback

    def add_mapper(self, name, callback):
        """
        Configures how `is_parallel` tasks get their inputs.
        """
        if name in self.mappers:
            raise ValueError('{} mapper already registered'.format(name))
        self.mappers[name] = callback

    def add_reducer(self, name, callback):
        """Configures how `is_parallel` tasks merge their output.

        .. warning:: Currently only one reducer is supported
        """
        if name in self.reducers:
            raise ValueError('{} reducer already registered'.format(name))
        self.reducers[name] = callback

    def run_pipeline(self, args, kwargs, **options):
        """
        Executes the pipeline and returns the chain of tasks used.

        Splits the pipeline into three steps:

        - Initial pipeline (executed as part of the current task)
        - Parallel pipeline (executed in separate tasks)
        - Finalize pipeline (executed in the final task)

        # TODO: how is the input/output handled
        # TODO: explain how tags work
        # TODO: explain how the task tree is generated

        :param args list: Arguments passed as an input to the pipeline.
        :param kwargs dict: Keyword arguments passed as an input to the pipeline.
        :param tagged_as list: TODO

        :returns: AsyncResult
        """
        no_arg = object()
        initial_arg = options.pop('initial_arg', no_arg)
        tasks = self._get_pipeline(args, kwargs, **options)
        if initial_arg is no_arg:
            return tasks.delay()
        return tasks.delay(initial_arg)

    def prettyprint_pipeline(self, args, kwargs, **options):
        """Stylish pipeline printout
        """
        tasks = self._get_pipeline(args, kwargs, **options)
        print tasks
        # TODO: draw the graph?

    def _get_pipeline(self, args, kwargs, **options):
        tagged_as = options.pop('tagged_as', [])

        # get tasks for default tag
        # Explicit dict for copy? amcm
        tasks = dict(self.registry[_sentinel])

        # override tasks by adding tasks in correct order
        for tag in tagged_as:
            if tag not in self.registry:
                raise ValueError('No pipelines for a tag {}'.format(tag))
            tasks.update(self.registry[tag])

        # now that we have the tasks, figure out the order of tasks
        tree = self.build_tree(tasks)

        # Make the signatures, so we can call the tasks
        self.add_signatures_to_graph(tree, args=args, kwargs=kwargs)

        # Reduce the tree by dependencies to task chain(s)
        celery_tasks = self.get_task_to_run(tree)

        # Chain to the final task if needed
        final = self.get_end_task(tasks, args, kwargs)
        if final is not None:
            celery_tasks |= final
        return celery_tasks

    def get_end_task(self, tasks, args, kwargs):
        """Accepts any number of tasks as returned by _get_pipeline.

        :param tasks: dictionary of str:info where str is the name of the task, info is from the registry
        :param args: list of args for each task
        :param kwargs: dict of kwargs for each task

        :returns: celery.group
        """

        sigs = [
            self.make_signature(info, args, kwargs)
            for name, info in tasks.items()
            if info['after'] is ALL
        ]

        if not sigs:
            return None

        return sigs[0] if len(sigs) == 1 else group(sigs)

    def build_tree(self, tasks):
        """Accepts any number of tasks as returned by _get_pipeline.

        :param tasks: dictionary of str:info where str is the name of the task, info is from the registry

        :returns: Graph containing each node connected by dependencies, "after: ALL" nodes will be ignored
        :rtype: networkx.DiGraph

        :raises: DependencyError
        """

        # Find dependencies - directed graph of node names
        tree = nx.DiGraph()

        # Add nodes
        for name, info in tasks.items():
            # TODO: doing this twice sucks
            if info['after'] is ALL:
                # ignore these
                continue
            tree.add_node(name, info=info)

        # Add edges
        for name, info in tasks.items():
            if info['after'] is ALL:
                # ignore these
                continue
            for req in info['after']:
                if req not in tree:
                    msg = 'after="{}" pipeline was not found: missing dependency from pipeline "{}" with configuration "{}"'
                    raise DependencyError(msg.format(req, name, info))
                tree.add_edge(req, name)

        # Not as useful as it originally seemed
        # tree = prune_edges(tree)

        # Check for circular dependencies
        try:
            cycle = nx.simple_cycles(tree).next()
            raise DependencyError('Circular dependencies detected: {}'.format(cycle))
        except StopIteration:
            # Good - didn't want any cycles
            pass

        # Joins (merge multiple tasks) have more than one edge in
        # joins = [n for n, d in tree.in_degree_iter() if d > 1]

        # Don't support joins right now, one reducer at the end of the chain
        # if joins:
        #    raise DependencyError('Multiple after values not currently supported joins="{}"'.format(joins))

        # TODO - even with joins this could be a challenge
        # Can't handle "N" shapes, so check for those
        # Descendants of forks, cannot join from outside.
        return tree

    def make_signature(self, info, args, kwargs):
        # Avoid circular import - used for map/reduce tasks
        from .tasks import lazy_async_apply_map

        new_kwargs = {k: v for k, v in kwargs.items() if k in info.get('requires_parameter', [])}
        task = info['func'].s(
            *args,
            **new_kwargs
        )

        # Check for mapper
        mapper_name = info.get('mapper')
        reducer_name = info.get('reducer')
        # If only one is defined, this is an error
        if bool(mapper_name) != bool(reducer_name):
            raise DependencyError(
                'Both mapper and reducer are required together info="{}"'.format(info))

        if mapper_name:  # implies reducer_name as well
            # This is a map/reduce task
            try:
                mapper = self.mappers[mapper_name]
            except KeyError:
                raise DependencyError('Missing mapper "{}"'.format(mapper_name))

            try:
                reducer = self.reducers[reducer_name]
            except KeyError:
                raise DependencyError('Missing reducer "{}"'.format(reducer_name))

            # lazy_async_apply_map must always be called in a chord for now, see:
            # https://github.com/celery/celery/issues/2722
            task = (
                mapper.s(*args, **new_kwargs) |
                chord(lazy_async_apply_map.s(task), reducer.s(*args, **new_kwargs))
            )
        return task

    def add_signatures_to_graph(self, tree, args, kwargs):
        """Add the 'task' key to data on each node of the graph to launch tasks
        as specified by the 'info' and arg, kwargs.

        :param tree: networkx.DiGraph with task info (will be modified)
        :param args: list of args for each task
        :param kwargs: dict of kwargs for each task

        :returns: None
        """

        # Make the signatures
        for name, data in tree.nodes(data=True):
            data['task'] = self.make_signature(data['info'], args, kwargs)

    def get_task_to_run(self, tree):
        """Working from the bottom up, replace each node with a chain to its
        descendant, or celery.Group of descendants.

        :param tree: Dependancy graph of tasks
        :type tree: networkx.DiGraph

        :returns: chain to execute
        """

        # TODO: This could be more parallel
        return chain(*[
            maybe_signature(tree.node[name]['task'], self.celery_app)
            for name in nx.topological_sort(tree)
        ])


def prune_edges(tree):
    tree = tree.copy()
    # Remove redundent edges
    # Given: edges((a, b), (b, c), (a, c))
    # Then : Edge (a, c) is not required
    #  As it is coverd by the path (a, b, c)
    for name, data in tree.nodes(data=True):
        for prereq in data['info']['after']:
            paths = list(nx.all_simple_paths(tree, prereq, name))
            if len(paths) > 1:
                tree.remove_edge(prereq, name)
    return tree
