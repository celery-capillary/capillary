from pytest import raises
from collections import defaultdict

import celery_capillary
from celery_capillary import make_pipeline_from_defaults, PipelineConfigurator, pipeline, _sentiel


class DummyScanner(object):
    def __init__(self):
        self.registry = defaultdict(dict)


def test_make_pipeline_from_defaults():
    assert make_pipeline_from_defaults()().__name__ == "decorator"


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


def test_callback():
    @pipeline()
    def bar():
        pass

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
        'reducer': None
    } == dummyscanner.registry[_sentiel]['bar']


def test_callback_name():
    @pipeline(name='foo')
    def bar():
        pass

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
        'reducer': None
    } == dummyscanner.registry[_sentiel]['foo']


def test_callback_twice():
    @pipeline()
    def bar():
        pass

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    with raises(ValueError):
        bar.callback(dummyscanner, 'bar', bar)


def test_callback_tags():
    @pipeline(tags=['A', 'B'])
    def bar():
        pass

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
        'reducer': None
    }
    # no default pipeline is registered
    assert dummyscanner.registry[_sentiel] == {}
    # but it is registered for each of the tags
    assert dummyscanner.registry['A']['bar'] == registered_pipeline
    assert dummyscanner.registry['B']['bar'] == registered_pipeline


def test_callback_tag_twice():
    @pipeline(tags=['A'])
    def bar():
        pass

    dummyscanner = DummyScanner()
    bar.callback(dummyscanner, 'bar', bar)
    with raises(ValueError) as excinfo:
        bar.callback(dummyscanner, 'bar', bar)
    assert str(excinfo.value) == '"bar" pipeline already exists for tag "A"'


def test_pipeline_wrong_arg():
    with raises(ValueError):
        pipeline(foobar=1)
