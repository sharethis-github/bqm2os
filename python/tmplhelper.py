import string
from datetime import datetime, timedelta


def evalTmplRecurse(templateKeys: dict):
    """
    We need to potentially format each of the value with some of the
    other values.  So some sort of recursion must happen i.e. we first
    find the k,v which are not templates and use them to format the
    unformatted values that we can.

    :param templateKeys: The values of the dict may be a template.
    :return: dict with same keys as templateKeys but fully formatted values
    """
    templateKeysCopy = templateKeys.copy()
    keysNeeded = {}
    usableKeys = {}

    for (k, v) in templateKeysCopy.items():
        keys = keysOfTemplate(v)
        if len(keys):
            keysNeeded[k] = keys
        else:
            usableKeys[k] = templateKeysCopy[k]

    while len(keysNeeded):
        remaining = len(keysNeeded)
        for (k, v) in templateKeysCopy.items():
            if k in usableKeys:
                continue

            needed = keysNeeded[k]
            if needed.issubset(usableKeys.keys()):
                templateKeysCopy[k] = templateKeysCopy[k].format(
                    **usableKeys)
                usableKeys[k] = templateKeysCopy[k]
                del keysNeeded[k]
        if remaining == len(keysNeeded):
            raise Exception("template vars: " + str(templateKeys) +
                            " contains a circular reference")

    for k, v in templateKeysCopy.items():
        if k.endswith("_dash2uscore"):
            templateKeysCopy[k] = templateKeysCopy[k].replace("-", "_")

    return templateKeysCopy


def keysOfTemplate(strr):
    if not isinstance(strr, str):
        return set()
    return set([x[1] for x in string.Formatter().parse(strr) if x[1]])


def handleDayDateField(dt: datetime, val) -> str:
    """
    val can be a string in which case we return it
    it can be an int in which case we evaluate it as a date that
    many days in the future or ago

    We may get more complicated in the future to support ranges, etc

    :return:
    """

    assert isinstance(dt, datetime)
    toFormat = []
    if isinstance(val, int):
        newdate = dt + timedelta(days=val)
        toFormat.append(newdate)
    elif isinstance(val, list) and len(val) == 2:
        val = sorted([int(x) for x in val])
        for v in range(int(val[0]), int(val[1]) + 1):
            newdate = dt + timedelta(days=v)
            toFormat.append(newdate)
    elif isinstance(val, str):
        return [val]
    else:
        raise Exception("Invalid datetime values to fill out.  Must "
                        "be int or 2 element array of ints")

    return sorted([dt.strftime("%Y%m%d") for dt in toFormat])


def explodeTemplate(templateVars: dict):
    """
    Goal of this method is simply to replace
    any array elements with simple string expansions

    :return:
    """
    # check for key with yyyymmdd and handle it specially
    for (k, v) in templateVars.items():
        if 'yyyymmdd' in k:
            templateVars[k] = handleDayDateField(datetime.today(), v)
        elif 'yyyymm' in k:
            dates = handleDayDateField(datetime.today(), v)
            for index in range(0, len(dates)):
                dates[index] = dates[index][0:6]
            dates = list(set(dates))
            templateVars[k] = dates

    topremute = []
    for (k, v) in templateVars.items():
        items = []
        if isinstance(v, list):
            for vv in v:
                items.append((k, vv))
        else:
            items.append((k, v))
        topremute.append(items)

    collect = []
    out = []
    makeCombinations(topremute, out, collect)
    # now make maps
    maps = []
    for s in collect:
        maps.append(dict(s))
    return maps


def makeCombinations(lists: list, out: list, collect: list):
    """
        given a list of lists, generate a list of lists which
        has all combinations of each element as a a member

        Example:
            [[a,b], [c,d]] becomes

            [
             [a,c],
             [a,d],
             [b,c],
             [b,d]
            ]
    """
    if not len(lists):
        collect.append(out)
        return

    listsCopy = lists.copy()
    first = listsCopy.pop(0)
    for m in first:
        outCopy = out.copy()
        outCopy.append(m)
        makeCombinations(listsCopy, outCopy, collect)
