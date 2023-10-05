import setuptools
import os

def read_text(file_name: str):
    return open(os.path.join(file_name)).read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

required = []
dependency_links = []

# Do not add to required lines pointing to Git repositories
EGG_MARK = '#egg='
for line in requirements:
    if line.startswith('-e git:') or line.startswith('-e git+') or \
            line.startswith('git:') or line.startswith('git+'):
        if EGG_MARK in line:
            package_name = line[line.find(EGG_MARK) + len(EGG_MARK):]
            required.append(package_name)
            dependency_links.append(line)
        else:
            print('Dependency to a git repository should have the format:')
            print('git+ssh://git@github.com/xxxxx/xxxxxx#egg=package_name')
    else:
        required.append(line)

setuptools.setup(
    name="tab2neo",                         # This is the name of the package
    version="2.0.3.0",                      # Release.Major Feature.Minor Feature.Bug Fix
    author="Alexey Kuznetsov",              # Full name of the author
    description="Clinical Linked Data: High-level Python classes to load, model and reshape tabular data imported into Neo4j database",
    long_description="https://github.com/GSK-Biostatistics/tab2neo/blob/main/README.md",      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(include=[
        "logger",
        "analysis_metadata",
        "derivation_method",
        "data_loaders",
        "data_providers",
        "model_appliers",
        "model_managers",
        "query_builders"
    ]),    # List of all python modules to be installed
    include_package_data=True,
    package_data={"derivation_method": ["*.cql"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    license=read_text("LICENSE"),
    python_requires='>=3.6',                # Minimum version requirement of the package
    # package_dir={'':''},                  # Directory of the source code of the package
    install_requires=required,
    dependency_links=dependency_links
)
