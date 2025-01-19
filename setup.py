from setuptools import setup, find_packages


# Read requirements from the requirements.txt file
def read_requirements(file_name="requirements.txt"):
    with open(file_name, "r") as file:
        return file.read().splitlines()


setup(
    name="data2deploy_test",  # Replace with your package name
    version="0.1.0",  # Replace with your version
    package_dir={"": "src"},  # Correct directory structure
    description="A description of my project",  # Short description
    long_description_content_type="text/markdown",  # Format of long description
    author="Your Name",  # Replace with your name
    author_email="your_email@example.com",  # Replace with your email
    url="https://github.com/username/my_project",  # Replace with your repo URL
    packages=find_packages(where="src"),  # Specify the source directory
    install_requires=read_requirements(),  # Dependencies from requirements.txt
    python_requires=">=3.10",  # Minimum Python version
    entry_points={
        "console_scripts": [
            "run-etl=etl.run_etl_aws:main",  # Example CLI command
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
