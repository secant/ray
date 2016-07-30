# Installation on Docker

You can install Ray on any platform that runs Docker. We do not presently publish Docker images for Ray, but you can build them yourself using the Ray distribution. Using Docker can provide a reliable way to get up and running quickly.

## Install Docker

The Docker Platform release is available for Mac, Windows, and Linux platforms. Please download the appropriate version from the [Docker website](https://www.docker.com/products/overview#/install_the_platform).

## Clone the Ray repository

```
git clone https://github.com/amplab/ray.git
```

## Build Docker images

Run the script to create Docker images.

```
cd ray
./build-docker.sh
```

This script creates two Docker images, one for general use and deployment and one for development use.

 * The `amplab/ray:deploy` has a self-contained copy of the code and is suitable for end users.
 * Ray developers who want to edit locally on the host filesystem should use the `amplab/ray:devel` image, which allows local changes to be reflected immediately within the container. 

## Launch Ray in Docker

Start out by launching the deployment container.

```
docker run -ti amplab/ray:deploy
```

## Test if the installation succeeded

To test if the installation was successful, try running some tests.

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```

You are now ready to continue with the [Tutorial](tutorial.md).

## Developing with Docker

These steps apply only to Ray developers who prefer to use editing tools on the host machine while building and running Ray within Docker. If you have previously been building locally we suggest that you start with a clean checkout before building with Ray's developer Docker container.

Launch the developer container.

```
docker run -v `pwd`:/home/ray/ray -ti amplab/ray:devel 
```

Build Ray inside of the container.

```
cd ray
./setup.sh
./build.sh
```