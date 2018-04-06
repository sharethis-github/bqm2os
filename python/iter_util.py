from time import sleep


def iterate(visitorFunc, iterFunc):
    """
    :param visitorFunc: The callback for each member of iteration.  Must
    return either True, to signal continued iteration or False, to signal
    a stop
    :param iterFunc: The iterator generator function which accepts an
    optional iter= parameter.   The iterFunc takes a previously generated
    iterator as state in order to determine whether or not to generate
    another iterator
    :return:
    """
    iter = iterFunc()

    while iter is not None:
        for t in iter:
            visitResult = visitorFunc(t)
            if visitResult is True:
                continue
            elif visitResult is False:
                return

            raise Exception("Illegal return value true or false."
                            "True means continue iteration.  "
                            "False means stop")
        iter = iterFunc(iter=iter)
