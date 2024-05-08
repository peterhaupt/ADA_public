from setuptools import setup, find_packages

setup(
    name="ADA_public",
    version="1.0.0",
    url="https://github.com/peterhaupt/ADA_public.git",
    author="Google",
    author_email="google@gmail.com",
    description="Pubsub",
    packages=find_packages(),
    install_requires=["google-cloud-pubsub"],
)
