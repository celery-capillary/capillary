.. highlight:: python

.. currentmodule:: celery_capillary

:Status: Alpha (API feedback is welcome, API could break compatibility)
:Generated: |today|
:Version: |release|
:License: CMG copyright
:Authors: Domen Kožar and Aaron McMillin


.. topic:: Introduction

      :mod:`celery_capillary` is a small integration package for
      the :mod:`celery` Distributed Task Queue with an aim of designing
      `workflows (Canvas) <http://celery.readthedocs.org/en/latest/userguide/canvas.html>`_
      in `a declarative manner <http://stackoverflow.com/questions/1784664/>`_
      using Python decorators.

      The main reason why :mod:`celery_capillary` exists is to get rid of manual
      tracking how celery tasks depend on each other.

      :mod:`celery_capillary` executes in two phases:

      1. Scans for all Celery tasks in defined Python packages
      2. Executes tasks based on metadata passed to :func:`@pipeline` decorators

      :mod:`celery_capillary` uses:

      - :mod:`venusian` to discover Celery tasks using deferred decorators
      - :mod:`networkx` to handle the graph operations for tracking
        dependencies between tasks in the workflow



User Guide
==========

.. _simple-example:

Simple Example
--------------

Below is the `first steps with Celery <http://celery.readthedocs.org/en/latest/getting-started/first-steps-with-celery.html#application>`_
tutorial expanded with :mod:`celery_capillary` integration.

The ``tasks.py`` module contains the steps of the pipeline.  Steps are
marked with the ``@pipeline`` decorator, which has optional parameters
to indicate that this step must be executed after some other step or
that the step has certain tags to allow groups of tasks to be executed together.

.. code-block:: python
    :linenos:

    from celery_capillary import pipeline

    @pipeline()
    def foo(celery_task):
        return 'simple task flow'

    @pipeline(after='foo')
    def bar(celery_task, l):
        return l.upper()

The ``myapp.py`` module then creates a :class:`PipelineConfigurator`
instance which will assemble the declared steps:

.. code-block:: python
    :linenos:

    from celery import Celery
    from celery_capillary import PipelineConfigurator

    import tasks

    app = Celery('tasks', broker='redis://', backend='redis://')
    pc = PipelineConfigurator(app)
    pc.scan(tasks)

Start the worker with

.. code-block:: bash

    $ celery worker -A myapp -D

and execute the pipeline in a Python shell:

.. code-block:: python
    :linenos:

    >>> from myapp import pc
    >>> asyncresult = pc.run()
    >>> asyncresult.get()
    SIMPLE TASK FLOW


*This example will be used throughout the user guide as a base.*

.. note::

    This example assumes the Redis broker is running with default settings, but any
    `Celery broker <http://celery.readthedocs.org/en/latest/getting-started/brokers/>`_
    will do.

    ``backend`` is defined only for retrieving the result using `.get()`, it is
    otherwise not required.


Core concept: Handling input and output parameters
--------------------------------------------------

Celery uses a concept called `partials <http://celery.readthedocs.org/en/latest/userguide/canvas.html#partials>`_
(sometimes also known as `Currying <https://en.wikipedia.org/wiki/Currying>`_) to
create function `signatures <http://celery.readthedocs.org/en/latest/userguide/canvas.html#signatures>`_.

:mod:`celery_capillary`  reuses these concepts to execute tasks. A value returned
from task ``foo`` is passed into task ``bar``.

It is possible to pass extra parameters to specific tasks as described in
:ref:`extra-parameters`.


Core concept: Tagging pipelines
-------------------------------

By default :meth:`PipelineConfigurator.run` will execute all scanned tasks
without tags in topological order.

If ``tags=['foobar']`` is passed to :func:`@pipeline`, the task will be run
when ```tagged_as=['foobar']`` is passed to :meth:`PipelineConfigurator.run`.

See :ref:`predefined_defaults` for information on how to reduce boilerplate and
group pipelines
per tag.


Aborting the Pipeline
---------------------

If a step needs to stop the current pipeline (meaning no further tasks
are processed in the pipeline), just raise :exc:`celery_capillary.AbortPipeline`
anywhere in your pipeline tasks.

.. _extra-parameters:

Passing extra parameters to a specific task
-------------------------------------------

Some :func:`@pipeline` elements might require extra arguments that are only
known when :meth:`PipelineConfigurator.run` is called.

.. code-block:: python

    >>> @pipeline(
    ...     required_kwarg_names=['param'],
    ... )
    ... def foobar(celery_task, param=None):
    ...     print param
    ...     return 'simple task flow'

When :meth:`PipelineConfigurator.run` is called, it will need `param` passed inside
`required_kwargs`; otherwise :exc:`MissingArgument` will be thrown.


Applying multiple :func:`@pipeline` decorators
----------------------------------------------

The most typical use case where two :func:`@pipeline` decorators are useful is when
you'd like to reuse a function for two different pipelines each differently tagged.

.. code-block:: python

    @pipeline(
        after=['first', 'second'],
        tags=['some_pipeline'],
    )
    @pipeline(
        after=['third'],
        tags=['other_pipeline'],
    )
    def foobar(celery_task):
        return 'simple task flow'


Executing ``ConfigurePipeline.run(tagged_as=['some_pipeline'])``
would run the `foobar` function as a task after `first` and `second` tasks were done.

However executing ``ConfigurePipeline.run(tagged_as=['other_pipeline'])``
would run the `foobar` function after `third` task was done.

.. note::

    If both tags are used (e.g. ``ConfigurePipeline.run(tagged_as=['some_pipeline', 'other_pipeline'])``)
    then ordering of tags specified matters and the latter will override a former.

    if you specify a different `name` parameter for each, they will be both executed.


.. _predefined_defaults:

Create pipelines based on predefined defaults
---------------------------------------------

Often :func:`@pipeline` definitions will repeat arguments through your
application. :func:`make_pipeline_from_defaults` allows you to create customized
predefined defaults for a pipeline. This example makes a ``foobar_pipeline``
decorator that will apply the same tag to each step:

.. code-block:: python

    >>> from celery_capillary import make_pipeline_from_defaults
    >>> foobar_pipeline = make_pipeline_from_defaults(
    >>>     tags=["foobar"]
    >>> )


Then use ``@foobar_pipeline`` just as one would use :func:`@pipeline` while all your
definitions will have `foobar` as a tag.

.. note::

    Passing ``tags`` to ``@foobar_pipeline`` will override ``["foobar"]`` value.


Printing the task tree
----------------------

To actually see what kind of canvas will be executed call
:meth:`ConfigurePipeline.prettyprint` with the same arguments as
:meth:`ConfigurePipeline.run`

.. code-block:: python

    >>> pc.prettyprint(args=[], kwargs={})
    tasks.foo() | tasks.bar()


The very last task in the pipeline
----------------------------------

Using a constant :class:`celery_capillary.ALL` it's possible to declare a task
as the last one in the pipeline

.. code-block:: python

      >>> from celery_capillary import ALL, pipeline
      >>> @pipeline(
      ...   after=ALL,
      ... )
      ... def last(celery_task, obj):
      ...    print('ALL DONE!')
      ...    return obj


.. note::

    Multiple tasks with `after=ALL` steps will be run
    in :class:`celery.group` as the last part of the pipeline.


Inner workings of :meth:`~PipelineConfigurator.run()`
-----------------------------------------------------

The following is a quick summary of what happens inside :meth:`~PipelineConfigurator.run()`:

- task tree is generated using dependency information
- Celery signatures are created
- task tree is reduced into a `chain <http://celery.readthedocs.org/en/latest/userguide/canvas.html#chains>`_
  using topological sort
- tasks is executed using :meth:`celery.app.Task.apply_async`

.. note::

    Currently the task tree is reduced into a linear chained list of tasks, but
    in future different "runners" could be implemented.


Unit Testing
------------

Functions marked as :func:`@pipeline` elements are still just simple untouched functions,
until :meth:`PipelineConfigurator.scan()` is called. If function code doesn't
depend on the first argument of ``celery_task``, just pass `None` as the value.

To unit test our two pipeline elements from :ref:`simple-example`:

.. code-block:: python

    class PipelineTestCase(unittest.TestCase):

        def test_bar(self):
            self.assertEquals(bar(None, 'test'), 'TEST')

        def test_foo(self):
            self.assertEquals(foo(None), 'simple task flow')


Development
===========

To run tests install `py.test` and run it:

.. code-block:: bash

    $ py.test tests/


Features to be considered
-------------------------

- Using a lot of tasks with large objects passed as arguments can be quite
  storage intensive. One alternative would be to generate signatures on-the-fly
  if Celery permits that.


API Reference
=============

.. automodule:: celery_capillary
    :members:
    :exclude-members: PipelineConfigurator

.. autoclass:: celery_capillary.PipelineConfigurator
    :members: run, prettyprint, scan
