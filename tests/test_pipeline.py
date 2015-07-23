import mock
import networkx as nx
from pytest import raises
from collections import defaultdict
from celery import chord

import celery_capillary
from celery_capillary import make_pipeline_from_defaults, PipelineConfigurator
from celery_capillary import pipeline, _sentinel
from celery_capillary.tasks import concat, dict_reducer, generator, lazy_async_apply_map


class DummyScanner(object):
    def __init__(self):
        self.registry = defaultdict(dict)
        self.celery_app = mock.Mock()
        self.celery_app.task().side_effect = lambda f: f


def test_make_pipeline_from_defaults():
    assert make_pipeline_from_defaults()().__code__ == pipeline().__code__


def test_add_error_handling_strategy():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_error_handling_strategy('foobar', callback)
    assert 'foobar' in pc.error_handling_strateies


def test_add_error_handling_strategy_twice():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_error_handling_strategy('foobar', callback)
    with raises(ValueError):
        pc.add_error_handling_strategy('foobar', callback)


def test_add_mapper():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_mapper('foobar', callback)
    assert 'foobar' in pc.mappers


def test_add_mapper_twice():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_mapper('foobar', callback)
    with raises(ValueError):
        pc.add_mapper('foobar', callback)


def test_add_reducer():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_reducer('foobar', callback)
    assert 'foobar' in pc.reducers


def test_add_reducer_twice():
    pc = PipelineConfigurator(None, celery_capillary)
    callback = lambda x: x
    pc.add_reducer('foobar', callback)
    with raises(ValueError):
        pc.add_reducer('foobar', callback)


def test_after_can_be_a_string():
    @pipeline(after='foo')
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    assert {
        'after': ['foo'],
        'error_handling_strategy': None,
        'func': bar,
        'is_parallel': False,
        'mapper': None,
        'name': 'bar',
        'reducer': None,
        'requires_parameter': [],
    } == dummyscanner.registry[_sentinel]['bar']


def test_requires_parameter_can_be_a_string():
    @pipeline(requires_parameter='foo')
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    assert {
        'after': [],
        'error_handling_strategy': None,
        'func': bar,
        'is_parallel': False,
        'mapper': None,
        'name': 'bar',
        'reducer': None,
        'requires_parameter': ['foo'],
    } == dummyscanner.registry[_sentinel]['bar']


def test_callback():
    @pipeline()
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    # then the pipeline should be registered under default tag and
    # under name bar
    assert {
        'after': [],
        'error_handling_strategy': None,
        'func': bar,
        'is_parallel': False,
        'mapper': None,
        'name': 'bar',
        'reducer': None,
        'requires_parameter': [],
    } == dummyscanner.registry[_sentinel]['bar']


def test_callback_name():
    @pipeline(name='foo')
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    # then the pipeline should be registered under default tag and
    # under name foo, not bar
    assert {
        'after': [],
        'error_handling_strategy': None,
        'func': bar,
        'is_parallel': False,
        'mapper': None,
        'name': 'foo',
        'reducer': None,
        'requires_parameter': [],
    } == dummyscanner.registry[_sentinel]['foo']


def test_callback_twice():
    @pipeline()
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    with raises(ValueError):
        bar.callback(dummyscanner, 'bar', bar)


def test_callback_tags():
    @pipeline(tags=['A', 'B'])
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    # then the pipeline should be registered under default tag and
    # under name foo, not bar
    registered_pipeline = {
        'after': [],
        'error_handling_strategy': None,
        'func': bar,
        'is_parallel': False,
        'mapper': None,
        'name': 'bar',
        'reducer': None,
        'requires_parameter': [],
    }
    # no default pipeline is registered
    assert dummyscanner.registry[_sentinel] == {}
    # but pipeline is registered for each of the tags
    assert dummyscanner.registry['A']['bar'] == registered_pipeline
    assert dummyscanner.registry['B']['bar'] == registered_pipeline


def test_callback_tag_twice():
    @pipeline(tags=['A'])
    def bar():
        pass  # pragma: no cover

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    with raises(ValueError) as excinfo:
        bar.callback(dummyscanner, 'bar', bar)
    assert str(excinfo.value) == '"bar" pipeline already exists for tag "A"'


def test_pipeline_wrong_arg():
    with raises(ValueError):
        pipeline(foobar=1)


def get_test_tree():
    pc = PipelineConfigurator(None, celery_capillary)
    g = nx.DiGraph()
    g.add_nodes_from([(n, {'task': concat.s(n)}) for n in xrange(1, 11)])
    g.add_edges_from([
        (1, 2),
        (2, 3),
        (2, 4),
        (4, 5),
        (6, 7),
        (6, 8),
        (8, 9),
        (9, 10),
    ])

    # When the graph is processed
    pc._condense_tree(g)
    # Then only 2 nodes remain
    assert len(g.nodes()) == 2

    return g


def test_condense_tree():
    # Given a directed graph with no joins, and 2 distinct subsets
    g = get_test_tree()

    # When the tasks are run
    res = []
    for node, data in g.nodes(data=True):
        res.append(data['task'].apply())

    # Then expected results occur
    # Results is a list of Group Results, each of which contains a list of EagerResults
    assert [group.get() for group in res] == [
        [[1, 2, 3], [1, 2, 4, 5]],
        [[6, 8, 9, 10], [6, 7]]
    ]

    # For further inspection
    return res
