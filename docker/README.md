# Radiology metadata

## Docker

This subdirectory will build a docker images with the dependencies you need to run the scripts in this repo.

### Building the image

```bash
cd docker
./build.sh "$my_docker_tag"  # e.g. pwrightkcl/rad-ext:20260101
```

The `build.sh` script takes one single argument - the Docker image tag - and builds the Docker image defined by `Dockerfile`. The image is based on a `python` base image and its critical dependencies are [pydicom](https://pydicom.github.io/) and [pynetdicom](https://pydicom.github.io/pynetdicom/), with [dcmtk](https://dicom.offis.de/en/) included for historical code.

You may not be able to build the image on a server inside an NHS trust firewall because the some trusts policy blocks access to the apt repos or pypi. You can work around this by building the image on your local machine and pushing it to a Docker registry, e.g. dockerhub. You can then pull the image to the NHS server (dockerhub is permitted at KCH and GSTT).

```bash
# Run on the server you intent to run the container
./add_user.sh "$root_tag" "$user_tag"
```

The `add_user.sh` script and `Dockerfile_add_user` should be run on the server where you intend to run the container. The script just adds your local user to the image, so it does not require internet access. It takes two arguments:

1. the tag of the base image you pulled
2. the new tag for the local image

#### Workaround if unable to add user

If the server you are working on does not allow you to run `add_user.sh`, you can work around this by running the root container, adding your user manually, and committing the changes to a new image.

```bash
# Terminal 1
id -un  # e.g. paulw
id -i  # e.g. 1234
id -g  # e.g. 4321
```
```bash
# Terminal 2
docker run -it --rm --name add_user "$root_tag"
# In the container
addgroup --gid 4321 paulw
useradd --shell /bin/bash --uid 1234 --gid 4321 paulw
```

```bash
# Terminal 1
docker commit add_user "$user_tag"  # This tag will be used to create a new, local image with your user added
```

```bash
# Terminal 2
exit  # This will stop and remove the temporary container.
```

### Running the image

```bash
# Start interactive container
docker run -it --rm --name rad-ext-int --user $USER --network host --shm-size=64G \
  -v /path/to/code:/path/to/code -v /path/to/data:/path/to/data \
  user/rad-ext:version
```

Arguments:

- `-it` makes the container interactive and starts a shell
- `--rm` removes the container when you exit
- `--user $USER` make sure you run as user not root
- `--network host` ensures the image can access the host machine's network directly, so it can connect to PACS
- `--shm-size=64G` increases the virtual memory allocated to the container to prevent out of memory errors when handling large data tables
- `-v` these are examples of local volumes you may wish to mount in your container
- `user/rad-ext:version` replace this with the tag you used when building your image

The entrypoint for the image is `/bin/bash` so if launched with no arguments for an interactive shell. Other parts of this repo will give instructions for running specific scripts in this container, including how to use a bash wrapper to run a Python script.