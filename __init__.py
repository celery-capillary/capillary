"""Usage

Configure the pipeline

>>> cp = PipelineConfigurator()
>>> cp.add_error_handling_strategy('exponential', lambda task: None)
>>> cp.add_mapper('related_objects', lambda f, x, s: s['related_objects'])
>>> cp.add_reducer('merge', lambda xsdm: None)

Create pipelines based on different defaults

>>> ap_pipeline = cp.make_pipeline_from_defaults(
>>>     part_of=["AP"]
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

>>> cp.run_pipeline(feedparser_feed_item, xml_feed_item, {}, part_of=['AP'])

"""
from functools import partial
from collections import defaultdict

import venusian
from structlog import get_logger


logger = get_logger()


def pipeline(**kwargs):
    """
    Decorator for configuring pipeline flow declaratively.

    :param name string: Unique identifier of the pipeline element (defaults to the decorated function name)
    :param part_of list:
    :param error_handling_strategy string: (only for is_parallel)
    :param is_parallel boolean: Should the pipeline element be ran in separate Celery task in parallel
    :param after list/string: On what pipeline elements does this element depend on
    :param mapper string/function: (only for is_parallel)
    :param reducer string/function: (only for is_parallel)
    :param task_kwargs dict: (only for is_parallel)
    """

    new_name = kwargs.pop('name', None)
    part_of = kwargs.pop('part_of', [])
    error_handling_strategy = kwargs.pop('error_handling_strategy', None)
    is_parallel = kwargs.pop('is_parallel', False)
    after = kwargs.pop('after', [])
    mapper = kwargs.pop('mapper', None)
    reducer = kwargs.pop('reducer', None)
    task_kwargs = kwargs.pop('task_kwargs', {})

    if kwargs:
        raise ValueError('@pipeline got unknown keyword parameters: {}'.format(kwargs))

    def decorator(wrapped):
        def callback(scanner, name, func):
            info = {
                'func': func,
                'name': new_name or name,
                'error_handling_strategy': error_handling_strategy,
                'is_parallel': is_parallel,
                'after': after,
                'mapper': mapper,
                'reducer': reducer,
                'task_kwargs': task_kwargs,
            }
            logger.debug('@pipeline args', name=name, info=info)
            if part_of:
                for part in part_of:
                    scanner.registry[part].append(info)
            else:
                scanner.registry['default'].append(info)
        venusian.attach(wrapped, callback, 'pipeline')
        return wrapped
    return decorator


def make_pipeline_from_defaults(**kw):
    """
    Changes default parameters to :meth:`ConfigurePipeline.pipeline` and
    returns a new decorator.
    """
    return partial(pipeline, **kw)


class PipelineConfigurator(object):
    """Interface for configuring a pipeline using Celery tasks.

    Supports mixing sync and async task within a pipeline.

    :param celery_app: Celery app being used as the base
    :param package: Where to scan for :meth:`ConfigurePipeline.pipeline` decorators

    """

    def __init__(self, celery_app, package):
        self.celery_app = celery_app
        self.error_handling_strateies = {}
        self.mappers = {}
        self.reducers = {}

        scanner = venusian.Scanner(registry=defaultdict(list))
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
        self.mapper[name] = callback

    def add_reducer(self, name, callback):
        """Configures how `is_parallel` tasks merge their output.

        .. warning:: Currently only one reducer is supported
        """
        if name in self.reducers:
            raise ValueError('{} reducer already registered'.format(name))
        self.reducers[name] = callback

    def run_pipeline(self, *args, **kwargs):
        """
        Executes the pipeline and returns the chain of tasks used.

        Splits the pipeline into three steps:

        - Initial pipeline (executed as part of the current task)
        - Parallel pipeline (executed in separate tasks)
        - Finalize pipeline (executed in the final task)

        # TODO: how is the output handled

        :param args: Arguments passed as an input to the pipeline.
        :param part_of list: TODO
        :returns: TODO

        .. warning:: Currently `run_pipeline` supports only one map/reduce
                     implemented as a chord in Celery.
        """
