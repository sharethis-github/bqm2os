import string
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from date_formatter_helper import helpers


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

    helpers.format_all_date_keys(templateKeysCopy)

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


def handleDateField(dt: datetime, val, key) -> str:
    """
    val can be a string in which case we return it
    it can be an int in which case we evaluate it as a date that
    many years/months/days/hours in the future or ago

    We may get more complicated in the future to support ranges, etc

    :return:
    """

    if not isinstance(dt, datetime):
        raise Exception("dt must be an instance of datetime")

    if key.endswith("yyyy"):
        func = relativedelta
        param = "years"
        format = "%Y"
    elif key.endswith("yyyymm"):
        func = relativedelta
        param = "months"
        format = "%Y%m"
    elif key.endswith("yyyymmdd"):
        func = timedelta
        param = "days"
        format = "%Y%m%d"
    elif key.endswith("yyyymmddhh"):
        func = timedelta
        param = "hours"
        format = "%Y%m%d%H"
    else:
        return None

    toFormat = []
    if isinstance(val, int):
        params = {param: val}
        newdate = dt + func(**params)
        toFormat.append(newdate)
    elif isinstance(val, list) and len(val) == 2:
        val = sorted([int(x) for x in val])
        for v in range(int(val[0]), int(val[1]) + 1):
            params = {param: v}
            newdate = dt + func(**params)
            toFormat.append(newdate)
    elif isinstance(val, str):
        return [val]
    else:
        raise Exception("Invalid datetime values to fill out.  Must "
                        "be int, 2 element array of ints, or string")

    return sorted([dt.strftime(format) for dt in toFormat])


def explodeTemplate(templateVars: dict):
    """
    Goal of this method is simply to replace
    any array elements with simple string expansions

    :return:
    """

    # check for key with yyyymm, yyyymmdd, or yyyymmddhh
    # and handle it specially
    for (k, v) in templateVars.items():
        date_vals = handleDateField(datetime.now(), v, k)
        if date_vals is not None:
            templateVars[k] = date_vals

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
