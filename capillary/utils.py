def merge_dicts(obj1, obj2):
    """Carries out an operation similar to obj1.update(obj2), but
    doesn't modify its input lists and will combine the values of lists
    and dicts contained within.

    :param dict obj1: First dictionary
    :param dict obj2: Second dictionary
    :returns: New dictionary with combined values
    :type: dict
    """

    def combine(v1, v2):
        if (isinstance(v1, collections.Mapping) and isinstance(v2, collections.Mapping)):
            return merge_dicts(v1, v2)

        elif isinstance(v1, collections.MutableSequence):
            # TODO - should this try to use sets to avoid duplicates?
            return list(v1) + list(v2)

        else:
            # Behave like dict.update(), and use the incoming dict's value.
            return copy.deepcopy(v2)

    result = copy.deepcopy(obj1)

    # Add the content of the second dict.
    for k, v in obj2.items():
        if k not in result:
            # Key wasn't present in the first dict, so we can just copy
            # the second dict's value.
            result[k] = copy.deepcopy(v)
        else:
            # Combine both values
            result[k] = combine(result[k], v)

    return result
