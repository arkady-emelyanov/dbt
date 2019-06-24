from dbt.clients.system import write_json

import dataclasses


class Replaceable:
    def replace(self, **kwargs):
        return dataclasses.replace(self, **kwargs)


class Writable:
    def write(self, path: str, omit_none: bool = True):
        write_json(path, self.to_dict(omit_none=omit_none))
