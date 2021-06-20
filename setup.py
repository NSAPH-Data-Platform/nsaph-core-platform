from setuptools import setup, find_packages

with open("README.md", "r") as readme:
    long_description = readme.read()


setup(
    name='nsaph',
    version="0.0.1.1",
    url='https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/nsaph',
    license='',
    author='Michael Bouzinier',
    author_email='mbouzinier@g.harvard.edu',
    description='Components of NSAPH Data Platform',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    #py_modules = [''],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Harvard University :: Development",
        "Operating System :: OS Independent"]
)
