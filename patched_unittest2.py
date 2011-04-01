import re
import unittest2
from mock import patch

__all__ = ['PatchedTestCase']

'''
This is an extension of unittest2 to work with Michael Foord's totally awesome Mock library.

The basic idea is that you want to do lots of patching and have the UT
mostly take care of it for you. For example:

WRITEME (I just got started here...)

class Car(object):
    def __init__(self, make):
        self.make = make

    def get_make(self):
        return self.make

class Driver(object):
    def __init__(self, vehicle):
        self.vehicle = vehicle

    def is_driving_a(self):
        return self.vehicle.get_make()

class TestDriver(PatchedTestCase): pass
@TestDriver.patch.object(Car, 'get_make')
class TestDriver(PatchedTestCase):
    def postSetUpPreRun(self):
        self.mock_Car_get_make.return_value = sentinel.that_car

    def test_is_driving_a

'''

class PatchType(type):
    """Metaclass for class-level mocking.

    This metaclass wraps any test method in the subclass so that any mock
    parameters passed in are pulled out and made into instance attributes.
    This keeps test_methods simple in that they don't have to have a long list
    of mock parameters, nor does the user have to keep track of the order
    """

    def __new__(mcs, name, bases, dict):
        for key in dict:
            if key.startswith('test'):
                dict[key] = mcs.patch_setup(dict[key])
        return type.__new__(mcs, name, bases, dict)

    @classmethod
    def patch_setup(cls, test_fn):
        """
        Wrapper around a test method that calls setup_patches() and
        subsequently calls the actual test method.
        """

        def test_runner(self, *args):
            self._setup_patches(args)
            self.postSetUpPreRun()

            try:
                test_fn(self)
            finally:
                self.postRunPreTearDown()
        return test_runner


class PatchedTestCase(unittest2.case.TestCase):
    """Base PatchTest class

    Keeps track of the attributes that have been patched.
    """

    # TODO: should Class level patches be setup at class instantiation and then mock.reset_mock()'d for each test method?

    __metaclass__ = PatchType

    pretty_attribute = re.compile(r'^(.*\.)?(?P<tail>[^.]+)$')

    # Keep track of patches.
    patches = {}

    def postSetUpPreRun(self):
        pass

    def postRunPreTearDown(self):
        pass

    def _setup_patches(self, patch_args):
        ''' Creates mock instance attributes.

        Naming is as follows:
        @patch_object(object, attr) gets mock_object_attr
        @patch_dict(dictname) gets mock_dict_dictname
        @patch(thing) gets mock_thing

        Yes, you could name things such that they clobber each other,
        and no, this module won't protect you against that.
        So... it's probably a good idea if you Don't Do That.
        '''

        self.assertEquals(len(self.patches[self.__class__.__name__]), len(patch_args))
        for (cls, attr), mock in zip(self.patches[self.__class__.__name__], reversed(patch_args)):
            m = self.pretty_attribute.match(attr)
            readable_attr = m.group('tail') if m else attr
            if cls is None:
                mock_name = 'mock_%s' % readable_attr
            else:
                mock_name = 'mock_%s_%s' % (cls.__name__, readable_attr)
            setattr(self, mock_name, mock)

    @classmethod
    def patch(cls, attr, **kwargs):
        ''' Wrapper around the mock module's @patch method.
        '''

        cls.patches.setdefault(cls.__name__, []).append((None, attr))
        return patch(attr, **kwargs)

    @classmethod
    def _patch_dict(cls, dict_name, **kwargs):
        cls.patches.setdefault(cls.__name__, []).append(('dict', dict_name))
        return patch.dict(dict_name, **kwargs)

    # the following is my uneducated guess at how to do this that doesn't work
    #patch.dict = _patch_dict

    @classmethod
    def patch_object(cls, obj, attr, **kwargs):
        '''Wrapper around the mock module's @patch.object method.

        In order to track the paches made, this method is called, which in turn
        calls the mock module.
        '''

        cls.patches.setdefault(cls.__name__, []).append((obj, attr))
        return patch.object(obj, attr, **kwargs)

    # I don't know how the Mock module gets away with this, but it doesn't work here.
    #patch.object = _patch_object
