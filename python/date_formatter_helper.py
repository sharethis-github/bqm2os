from datetime import datetime


class DateFormatHelper:
    def __init__(self, formats: list, formats_suffixes: list):
        """
        :param formats: datetime formats
        :param formats_suffixes: template key suffixes or endings
        """
        self.formats = formats
        self.formats_suffixes = formats_suffixes

        assert len(formats)
        assert len(formats) == len(formats_suffixes)

    def format_date_key(self, k: str, v: str, m: dict):
        if k.endswith(f"_{self.formats_suffixes[0]}") \
                or k == self.formats_suffixes[0]:
            for i in range(1, len(self.formats_suffixes)):
                newkey = k.replace(self.formats_suffixes[0],
                                   self.formats_suffixes[i])
                if newkey in m:
                    continue
                newval = datetime.strptime(v, self.formats[0])\
                    .strftime(self.formats[i])
                m[newkey] = newval

    def show_new_keys(self, keys: list):
        m = set()
        for k in keys:
            if k == self.formats_suffixes[0] \
                    or k.endswith(f"_{self.formats_suffixes[0]}"):
                for i in range(1, len(self.formats)):
                    m.add(k.replace(self.formats_suffixes[0],
                                    self.formats_suffixes[i]))
        return m


class DateFormatHelpers:
    def __init__(self, formatters):
        self.formatters = formatters
        assert len(formatters)

    def show_new_keys(self, keys: list):
        assert isinstance(keys, list)
        ret = set()
        for f in self.formatters:
            ret.update(f.show_new_keys(keys))
        return ret

    def format_date_keys(self, k: str, v: str, m: dict):
        for f in self.formatters:
            f.format_date_key(k, v, m)

    def format_all_date_keys(self, m: dict):
        for f in self.formatters:
            kv = [x for x in m.items()]
            for x in kv:
                try:
                    f.format_date_key(x[0], x[1], m)
                except ValueError as e:
                    raise ValueError(f"Unable to format "
                                     f"key/value "
                                     f"{x[0]}/{x[1]}: {e}")


helpers = DateFormatHelpers(
    [
        DateFormatHelper(["%Y%m%d%H", "%Y", "%m", "%d", "%H"],
                         ["yyyymmddhh", "yyyymmddhh_yyyy",
                          "yyyymmddhh_mm", "yyyymmddhh_dd",
                          "yyyymmddhh_hh"]),
        DateFormatHelper(["%Y%m%d", "%Y", "%m", "%d", '%y'],
                         ["yyyymmdd", "yyyymmdd_yyyy",
                          "yyyymmdd_mm", "yyyymmdd_dd", "yyyymmdd_yy"]),
        DateFormatHelper(["%Y%m", "%Y", "%m"],
                         ["yyyymm", "yyyymm_yyyy", "yyyymm_mm"]),
    ]
)
