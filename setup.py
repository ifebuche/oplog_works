from setuptools import setup, find_packages


with open("README.md", "r") as f:
    docs = f.read()

with open('requirements3.txt', "r") as s:
    required = s.read().splitlines() 

setup(
    name='MI_ETLx',
    version='0.0.35',
    author=['Paschal Amah', 'Nelson Ogbeide', 'Joseph Ojo'],
    author_email='ojofemijoseph@gmail.com, ogbeide331@gmail.com, agaley.fesh@gmail.com',
    # description=,
    long_description=docs,
    long_description_content_type='text/markdown',
    url='https://github.com/ifebuche/oplog_works',
    download_url = 'https://github.com/ifebuche/oplog_works',
    packages=find_packages(exclude=["tests*", "docs*"]),
    install_requires=required,
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.org/classifiers/
        'Development Status :: 4 - Beta', 
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        
    ],
    License="MIT",
    python_requires='>=3.8',  # Minimum version requirement of the package
    entry_points={
        # Optional
        # Entry points for your package, e.g., console scripts
        # 'console_scripts': ['your_command=your_package.module:function', ],
    },
)
