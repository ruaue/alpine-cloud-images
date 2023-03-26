# vim: ts=4 et:

class DictObj(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        del self[key]


class ImageTags(DictObj):

    def __init__(self, d={}, from_list=None, key_name='Key', value_name='Value'):
        for key, value in d.items():
            self.__setattr__(key, value)

        if from_list:
            self.from_list(from_list, key_name, value_name)

    def __setattr__(self, key, value):
        self[key] = str(value)

    def as_list(self, key_name='Key', value_name='Value'):
        return [{key_name: k, value_name: v} for k, v in self.items()]

    def from_list(self, list=[], key_name='Key', value_name='Value'):
        for tag in list:
            self.__setattr__(tag[key_name], tag[value_name])
