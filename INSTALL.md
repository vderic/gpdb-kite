#install python

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


