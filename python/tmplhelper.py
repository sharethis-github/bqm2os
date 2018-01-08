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
    return templateKeysCopy


def keysOfTemplate(x):
    if isinstance(x, list):
        ret = set([])
        [ret.add(y) for y in [z for z in x]]
    return set([x[1] for x in string.Formatter().parse(str) if x[1]])


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


def computeImpliedRequiredVars(requiredVars: set, templateVars: dict):
    """

    :param requiredVars: A template declares it needs vars x, y, z
     but to actually compute those , we may may need vars t, u, v which are found
     in templateVars, but ....
     we don't want to do cross product expansion of a var in templateVars if it's not
     actually required in the template to begin with.

    :param templateVars:
    :return:
    """
    requiredVars = requiredVars.copy()
    toReturn = requiredVars.copy()

    # todo: handle key requesting key i.e. "val": "{val}"
    while len(requiredVars):
        next = requiredVars.pop()
        toReturn.add(next)
        # todo: what if requiredVars is not contained in templateVars.keys()?
        requested = keysOfTemplate(templateVars[next])
        for r in requested:
            if r not in toReturn:
                toReturn.add(r)
                requiredVars.add(r)

    return toReturn

def explodeTemplateVarsArray(requiredVars: set,
                              rawTemplates: list,
                              defaultVars: dict):
    ret = []
    for t in rawTemplates:
        copy = t.copy()
        for (k, v) in defaultVars.items():
            if k not in copy:
                copy[k] = v

        # remove any unneeded keys
        impliedRequired = computeImpliedRequiredVars(requiredVars, copy)
        copy = { k: v for k, v in copy.items() if k in impliedRequired}
        ret += [evalTmplRecurse(t) for t in explodeTemplate(copy)]

    return ret