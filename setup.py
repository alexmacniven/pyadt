"""This is a template setup file, hopefully you'll only need to change
the meta-data"""
import codecs
import os
import sys

from setuptools import find_packages, setup, Command
from shutil import rmtree


# Package meta-data
NAME = "pyadt"
DESCRIPTION = "A wrapper for working with ADT tables in Python 3.7"
URL = 'https://bitbucket.org/sunshinetechnology/pyadt'
EMAIL = "apmacniven@outlook.com"
AUTHOR = "Alex Macniven"

# What packages are required for this module to be executed?
REQUIRED = [
    'pyodbc'
]

# Import the README and use it as the long-description
# Note: README.md needs to be in the MANIFEST
here = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

# Load the package's __version__.py module as a dictionary
# Note: A seperate __version__.py is used so you can add imports to the
# __init__.py file
about = {}
with open(os.path.join(here, NAME, '__version__.py')) as f:
    exec(f.read(), about)

# The Initialize class allows a user to setup the host machine with the
# necessary files and configurations.
class RunTests(Command):
    """Sets up the host machine with any configurations."""

    description = "Run all tests."
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from subprocess import call
        errno = call(["python", "-m", "unittest", "discover"])
        raise SystemExit(errno)

class UploadCommand(Command):

    """Support setup.py publish."""
    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except FileNotFoundError:
            pass

        self.status("Building Source distribution…")
        os.system("{0} setup.py sdist bdist_wheel".format(sys.executable))
        self.status("Uploading the package to PyPI via Twine…")
        os.system("twine upload dist/*")
        self.status("Pushing git tags…")
        os.system("git tag v{0}".format(about["__version__"]))
        os.system("git push --tags")
        sys.exit()

# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],

    # If the package is not intended to be used from the console, you
    # can comment out 'entry_points'
    # entry_points={
    #     'console_scripts': ['{0}={0}.cli:main'.format(NAME)],
    # },
    install_requires=REQUIRED,
    include_package_data=True,
    license='ISC',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: ISC License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
    cmdclass={
        'test': RunTests,
        'upload': UploadCommand
    }
)
