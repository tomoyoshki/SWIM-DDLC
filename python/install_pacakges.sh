sudo pip3 install --upgrade pip
sudo python3 -m pip install --no-cache-dir -r requirements.txt
wget -O python/resnet50.pth https://download.pytorch.org/models/resnet50-0676ba61.pth
wget -O python/resnet18.pth https://download.pytorch.org/models/resnet18-f37072fd.pth