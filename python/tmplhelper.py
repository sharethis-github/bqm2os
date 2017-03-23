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


def keysOfTemplate(str):
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

    striped = {}
    output_length = 1
    for (k, v) in templateVars.items():
        if isinstance(v, list):
            output_length *= len(v)

    for (k, v) in templateVars.items():
        if isinstance(v, list):
            mult = int(output_length / len(v))
            striped[k] = v * mult
        else:
            striped[k] = [v] * output_length

    ret = []
    for i in range(output_length):
        element = {}
        for (k, v) in striped.items():
            element[k] = striped[k][i]

        ret.append(element)

    return ret
