Install GPDB7
=============

## Compile python from source

```
sudo apt-get install libffi-dev

wget https://www.python.org/ftp/python/3.10.11/Python-3.10.11.tgz

sudo ./configure --enable-shared --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi --prefix=/opt/python/3.10.11/

make -j8

sudo make altinstall

# symlink python3 to new python

python3 --version

wget https://bootstrap.pypa.io/get-pip.py

python3 get-pip.py --user

pip3 install psutil
pip3 install PyGreSQL

cd /usr/lib/x86_64-linux-gnu/
sudo ln -s /opt/python/3.10.11/lib/libpython3.10.so libpython3.10.so
sudo ln -s /opt/python/3.10.11/lib/libpython3.10.so.1.0 libpython3.10.so.1.0
sudo ln -s /opt/python/3.10.11/lib/libpython3.so libpython3.so

```

## For Ubuntu:

- Install Dependencies When you run the README.Ubuntu.bash script for dependencies, you will be asked to configure realm for kerberos. You can enter any realm, since this is just for testing, and during testing, it will reconfigure a local server/client. If you want to skip this manual configuration, use: export DEBIAN_FRONTEND=noninteractive

```
sudo ./README.Ubuntu.bash
```

- Ubuntu 18.04 and newer should have use gcc 7 or newer, but you can also enable gcc-7 on older versions of Ubuntu:

```
sudo add-apt-repository ppa:ubuntu-toolchain-r/test -y
sudo apt-get update
sudo apt-get install -y gcc-7 g++-7
```

## Build the database

```
# Initialize and update submodules in the repository
git submodule update --init

# Configure build environment to install at /usr/local/gpdb
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb

# Compile and install
make -j8
make -j8 install

# Bring in greenplum environment into your running shell
source /usr/local/gpdb/greenplum_path.sh

```
