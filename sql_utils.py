# Functions for reading scripts
class ScriptReader(object):

    @staticmethod
    def get_script(path):
        return open(path, 'r').read()