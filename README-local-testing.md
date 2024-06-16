These local tests assume they will be run in a local virtual python environment
These tests allow a developer to run some airflow logic tests locally without the need for airflow and docker.
These tests only take a fraction of the time it takes to run the whole docker test suite

Establish virtual environment with venv
 - python3 -m venv .venv

To activate virtual environment for shell session
 - `source .venv/bin/activate`


Tendril repositories are required for these tests, add them as repository index locations with a PIP package manager config file
 - create `.venv/pip.conf`
 - login with jumpcloud creds to `https://tendril.jfrog.io/tendril/webapp/#/home`
 - Click on `Username` -> `Profile Settings` and create API key
 - Click on Artifactory -> Artifacts -> `Set Me Up`
 - Select tool `PyPi`,  repository `pypi-virtual`
 - login with password again
 - copy the [global] string to .venv/pip.conf
 - change `index-url` to `extra-index-url`

```
[global]
extra-index-url = https://user.name:******@tendril.jfrog.io/tendril/api/pypi/pypi-virtual/simple
timeout = 10
```

Make sure you have python 3.5.6 installed

```
pyenv install 3.5.6
```

Run tests with desired test suite eg

```
./scripts/test_local.sh nba
```

Currently supported tests
 - nba - Next Best Action

No arguments will run all currently supported tests

Run ./scripts/test_local.sh