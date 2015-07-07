"""Example usage

Configure the pipeline

>>> from celery_capillary import PipelineConfigurator
>>> pc = PipelineConfigurator()
>>> pc.add_error_handling_strategy('exponential', lambda task: None)
>>> pc.add_mapper('related_objects', lambda f, x, s: s['related_objects'])
>>> pc.add_reducer('merge', lambda xsdm: None)

Create pipelines based on different defaults

>>> from celery_capillary import make_pipeline_from_defaults
>>> ap_pipeline = make_pipeline_from_defaults(
>>>     tags=["AP"]
>>> )


Parallel pipeline

>>> @ap_pipeline(
>>>     error_handling_strategy="backoff",
>>>     after="related_objects",
>>>     mapper="related_objects",
>>>     is_parallel=True,
>>>     reducer="merge",
>>> )
>>> def upload_image(self, feedparser_feed_item, xml_feed_item, sdm):
>>>     guid = sdm['guid']
>>>     response = requests.get(guid)
>>>     return {'related_item': {'metadata': response}}

Basic usage

>>> @ap_pipeline(
>>>     after=["body_content"],
>>> )
>>> def summary(self, feedparser_feed_item, xml_feed_item, sdm):
>>>     return {'summary': sdm['body_content'][:30]}

Execute the pipeline
>>> pc.run_pipeline(feedparser_feed_item, xml_feed_item, {}, tagged_as=['AP'])

"""

# TODO: How to abort the pipeline? Just raise "AbortPipeline" and log it

from functools import partial
from collections import defaultdict

import venusian
from structlog import get_logger


logger = get_logger()
_sentiel = object()


class AbortPipeline(Exception):
    """Raised if the pipeline should be aborted all together."""


def pipeline(**kwargs):
    """
    Decorator for configuring pipeline flow declaratively.

    :param name string: Unique identifier of the pipeline element (defaults to the decorated function name)
    :param tags list:
    :param error_handling_strategy string: (only for is_parallel)
    :param is_parallel boolean: Should the pipeline element be ran in separate Celery task in parallel
    :param after list/string: On what pipeline elements does this element depend on
    :param mapper string/function: (only for is_parallel)
    :param reducer string/function: (only for is_parallel)

    Example::

    >>> @pipeline(
    >>>     after="somepipeline",
    >>> )
    >>> def foobar(self, *args):
    >>>     raise NotImplemented

    """

    new_name = kwargs.pop('name', None)
    tags = kwargs.pop('tags', [])
    error_handling_strategy = kwargs.pop('error_handling_strategy', None)
    is_parallel = kwargs.pop('is_parallel', False)
    after = kwargs.pop('after', [])
    mapper = kwargs.pop('mapper', None)
    reducer = kwargs.pop('reducer', None)

    if kwargs:
        raise ValueError('@pipeline got unknown keyword parameters: {}'.format(kwargs))

    def decorator(wrapped):
        def callback(scanner, name, func):
            name = new_name or name
            info = {
                'func': func,
                'name': name,
                'error_handling_strategy': error_handling_strategy,
                'is_parallel': is_parallel,
                'after': after,
                'mapper': mapper,
                'reducer': reducer,
            }
            logger.debug('@pipeline registered', name=name, info=info, tags=tags)
            if tags:
                for tag in tags:
                    tagged = scanner.registry[tag]
                    if name in tagged:
                        raise ValueError('"{}" pipeline already exists for tag "{}"'.format(name, tag))
                    tagged[name] = info
            else:
                untagged = scanner.registry[_sentiel]
                if name in untagged:
                    raise ValueError('{} pipeline already exists without tags'.format(name))
                untagged[name] = info
        venusian.attach(wrapped, callback, 'pipeline')
        wrapped.callback = callback  # for testing purposes
        return wrapped
    return decorator


def make_pipeline_from_defaults(**kw):
    """
    Changes default parameters to :meth:`ConfigurePipeline.pipeline` and
    returns a new decorator.

    Example::

    >>> ap_pipeline = make_pipeline_from_defaults(
    >>>     tags=["AP"]
    >>> )

    >>> @ap_pipeline(
    >>>     after="somepipeline",
    >>> )
    >>> def foobar(self, *args):
    >>>     raise NotImplemented

    """
    return partial(pipeline, **kw)


class PipelineConfigurator(object):
    """Interface for configuring a pipeline using Celery tasks.

    Supports mixing sync and async task within a pipeline.

    :param celery_app: Celery app being used as the base
    :param package: Where to scan for :func:`pipeline` decorators

    """

    def __init__(self, celery_app, package):
        self.celery_app = celery_app
        self.error_handling_strateies = {}
        self.mappers = {}
        self.reducers = {}

        scanner = venusian.Scanner(registry=defaultdict(dict))
        scanner.scan(package, categories=['pipeline'])
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

        # TODO: how is the output handled

        :param args: Arguments passed as an input to the pipeline.
        :param kwargs: Keyword arguments passed as an input to the pipeline.
        :param tagged_as list: TODO
        :returns: TODO

        .. warning:: Currently `run_pipeline` supports only one map/reduce
                     implemented as a chord in Celery.
        """
        tasks = self._get_pipeline(**options)

    def prettyprint_pipeline(self, args, kwargs, **options):
        """
        TODO
        """
        tasks = self._get_pipeline(**options)
        print tasks

    def _get_pipeline(self, **options):
        tagged_as = options.pop('tagged_as', [])

        # get tasks for default tag
        tasks = dict(self.registry[_sentiel])

        # override tasks by adding tasks in correct order
        for tag in tagged_as:
            if tag not in self.registry:
                raise ValueError('No pipelines for a tag {}'.format(tag))
            tasks.update(self.registry[tag])

        # TODO: now that we have the tasks, figure out the order of tasks
