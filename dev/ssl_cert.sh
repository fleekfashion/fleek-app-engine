sudo add-apt-repository ppa:certbot/certbot
sudo apt update
sudo apt install python-certbot-nginx
sudo systemctl reload nginx
sudo certbot --nginx -d fleek-fashion-engine.shop 

