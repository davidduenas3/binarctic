import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="binarctic", 
    version="0.0.1",
    author="davidduenas3",
    author_email="david.duenas3@gmail.com",
    description="binance+arctic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/davidduenas3/binarctic",
    packages=setuptools.find_packages(exclude=['tests',]),
    exclude_package_data={
        "":[".gitignore",".gitmodules"]
    },
    license="GPL",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    zip_safe=True,
)
